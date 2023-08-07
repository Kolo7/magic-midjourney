package sse

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	discord "github.com/bwmarrin/discordgo"
)

type DiscordAction string

const (
	Begin  DiscordAction = "Begin"
	Update DiscordAction = "Update"
	End    DiscordAction = "End"
	Error  DiscordAction = "Error"
)

type DiscordActMessage struct {
	Message discord.Message
	Action  DiscordAction
}

var (
	// DiscordMessageCh = make(chan *DiscordActMessage, 1)

	// 并发安全的map，用于存储不同的msgCh
	MsgChManager = MsgChMap{
		Map: &sync.Map{},
	}
)

func SSE(w http.ResponseWriter, r *http.Request, ch chan *DiscordActMessage) {
	// 设置超时时间
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	// 设置一个定时器
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	serial := 0

	es := eventsource.New(nil, nil)
	defer es.Close()
	es.ServeHTTP(w, r)

	for {
		serial++
		select {
		case <-ctx.Done():
			// 打印退出信息
			log.Println("client closed")
			return
		case <-ticker.C:
			// 每秒钟发送一次消息
			es.SendEventMessage(`{"msg":"wait"}`, "data", strconv.Itoa(serial))
		case msg := <-ch:
			if msg.Action == End {
				chID, _, err := UnwrapMsg(msg.Message.Content)
				if err != nil {
					fmt.Println("UnwrapMsg error: ", err)
					return
				}

				resp, err := http.Get(msg.Message.Attachments[0].URL)
				if err != nil {
					fmt.Println("HTTP请求错误:", err)
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					fmt.Println("HTTP请求错误，响应状态码:", resp.StatusCode)
					return
				}
				// 获取body的大小
				size := resp.ContentLength
				uploadRes, err := qiniu_cloud(msg.Message.Attachments[0].URL, resp.Body, size)
				if err != nil {
					fmt.Println("上传文件错误:", err)
					return
				}
				url := uploadRes.PublicAccessURL

				es.SendEventMessage(fmt.Sprintf(`{"url":"%s","id":"%s","msgHash":"%s","sessionID":"%s"}`,
					url, msg.Message.ID, getLastString(msg.Message.Attachments[0].Filename), chID),
					"data", strconv.Itoa(serial))
				return
			}
			if msg.Action == Begin {
				es.SendEventMessage(`{"msg":"begin"}`, "data", strconv.Itoa(serial))
			}
			if msg.Action == Update {
				resp, err := http.Get(msg.Message.Attachments[0].URL)
				if err != nil {
					fmt.Println("HTTP请求错误:", err)
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					fmt.Println("HTTP请求错误，响应状态码:", resp.StatusCode)
					return
				}
				// 获取body的大小
				size := resp.ContentLength
				uploadRes, err := qiniu_cloud(msg.Message.Attachments[0].URL, resp.Body, size)
				if err != nil {
					fmt.Println("上传文件错误:", err)
					return
				}
				url := uploadRes.PublicAccessURL

				es.SendEventMessage(fmt.Sprintf(`{"url":"%s","id":"%s"}`,
					url, msg.Message.ID), "data", strconv.Itoa(serial))
			}
		}
	}
}

// 将字符串按_以及.分割，然后返回最后一个字符串
func getLastString(s string) string {
	split := strings.Split(s, "_")
	return strings.Split(split[len(split)-1], ".")[0]
}

type MsgChMap struct {
	*sync.Map
}

// 在map中增加一个msgCh，key为一个唯一id
func (m *MsgChMap) AddMsgCh(msgCh chan *DiscordActMessage) string {
	// id 为当前纳秒时间戳+随机数
	id := fmt.Sprintf("%d%d", time.Now().UnixNano(), rand.Intn(1000))
	m.Store(id, msgCh)
	return id
}

// 在map中增加一个msgCh，key为一个唯一id
func (m *MsgChMap) AddMsgCh1ID(id string, msgCh chan *DiscordActMessage) string {
	// id 为当前纳秒时间戳+随机数
	m.Store(id, msgCh)
	return id
}

// 从map中删除一个msgCh
func (m *MsgChMap) DelMsgCh(id string) {
	m.Delete(id)
}

// 从map中获取一个msgCh
func (m *MsgChMap) GetMsgCh(id string) (chan *DiscordActMessage, bool) {
	msgCh, ok := m.Load(id)
	if !ok {
		return nil, false
	}
	return msgCh.(chan *DiscordActMessage), true
}

// 将信息进行包装，加上id(<!id:唯一标识符>)
func WrapMsg(msg string, id string) string {
	return fmt.Sprintf("<!id:%s> %s", id, msg)
}

// 将信息进行解包，返回id以及信息
func UnwrapMsg(msg string) (id string, body string, err error) {
	// 通过正则表达式判断是否符合格式
	rul := `<!id:\d+> .+$`
	reg := regexp.MustCompile(rul)
	if !reg.MatchString(msg) {
		return "", "", fmt.Errorf("msg %s not match rule %s", msg, rul)
	}

	// 截断到第一个空格
	split := strings.SplitN(msg, " ", 2)
	// 去掉前面的<!id:和后面的>
	id = strings.TrimSuffix(strings.TrimPrefix(split[0], "**<!id:"), ">")
	return id, split[1], nil
}
