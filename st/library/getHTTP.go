package library

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/nytlabs/gojee"                 // jee
	"github.com/nytlabs/streamtools/st/blocks" // blocks
	"github.com/nytlabs/streamtools/st/util"
)

// specify those channels we're going to use to communicate with streamtools
type GetHTTP struct {
	blocks.Block
	queryrule chan blocks.MsgChan
	inrule    blocks.MsgChan
	in        blocks.MsgChan
	out       blocks.MsgChan
	quit      blocks.MsgChan
}

// we need to build a simple factory so that streamtools can make new blocks of this kind
func NewGetHTTP() blocks.BlockInterface {
	return &GetHTTP{}
}

// Setup is called once before running the block. We build up the channels and specify what kind of block this is.
func (b *GetHTTP) Setup() {
	b.Kind = "GetHTTP"
	b.Desc = "makes an HTTP GET request to a URL you specify in the inbound message"
	b.in = b.InRoute("in")
	b.inrule = b.InRoute("rule")
	b.queryrule = b.QueryRoute("rule")
	b.quit = b.Quit()
	b.out = b.Broadcast()
}

// Run is the block's main loop. Here we listen on the different channels we set up.
func (b *GetHTTP) Run() {
	client := &http.Client{}
	var tree *jee.TokenTree
	var path, headerString string
	var err error
	headers := make(map[string]string)
	for {
		select {
		case ruleI := <-b.inrule:
			// set a parameter of the block
			path, err = util.ParseString(ruleI, "Path")
			if err != nil {
				b.Error(err)
				continue
			}
			token, err := jee.Lexer(path)
			if err != nil {
				b.Error(err)
				continue
			}
			tree, err = jee.Parser(token)
			if err != nil {
				b.Error(err)
				continue
			}
			headerString, err = util.ParseString(ruleI, "Header")
			if err != nil {
				b.Error(err)
				continue
			}
			for _, headerpair := range strings.Split(headerString, ",") {
				hh := strings.Split(headerpair, ":")
				headers[hh[0]] = hh[1]
			}
		case <-b.quit:
			// quit the block
			return
		case msg := <-b.in:
			// deal with inbound data
			if tree == nil {
				continue
			}
			urlInterface, err := jee.Eval(tree, msg)
			if err != nil {
				b.Error(err)
				continue
			}
			urlString, ok := urlInterface.(string)
			if !ok {
				b.Error(errors.New("couldn't assert url to a string"))
				continue
			}

			req, err := http.NewRequest("GET", urlString, nil)
			for header := range headers {
				req.Header.Add(header, headers[header])
			}

			resp, err := client.Do(req)
			if err != nil {
				b.Error(err)
				continue
			}
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				b.Error(err)
				continue
			}
			var outMsg interface{}
			// try treating the body as json first...
			err = json.Unmarshal(body, &outMsg)

			// if the json parsing fails, store data unparsed as "data"
			if err != nil {
				outMsg = map[string]interface{}{
					"data": string(body),
				}
			}
			b.out <- outMsg
		case MsgChan := <-b.queryrule:
			// deal with a query request
			MsgChan <- map[string]interface{}{
				"Path":   path,
				"Header": headerString,
			}

		}
	}
}
