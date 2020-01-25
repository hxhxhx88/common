package wechat

import (
	"fmt"
	"net/http"

	"github.com/hxhxhx88/common/web"
)

// JSLoginResp ...
type JSLoginResp struct {
	OpenID     string `json:"openid"`
	UnionID    string `json:"unionid"`
	SessionKey string `json:"session_key"`
	ErrCode    int    `json:"errcode"`
	ErrMsg     string `json:"errmsg"`
}

// JSLogin ...
func JSLogin(appid, appsecret, code string) (resp JSLoginResp, err error) {
	url := fmt.Sprintf("https://api.weixin.qq.com/sns/jscode2session?appid=%s&secret=%s&js_code=%s&grant_type=authorization_code",
		appid,
		appsecret,
		code,
	)

	wechatResp, err := http.Get(url)
	if err != nil {
		return
	}

	if err = web.ReadJSONBody(wechatResp.Body, &resp); err != nil {
		return
	}

	if resp.OpenID == "" {
		err = fmt.Errorf("errcode: %d, errmsg: %s", resp.ErrCode, resp.ErrMsg)
		return
	}

	return
}
