package parser

import (
	"bufio"
	"bytes"
	"io"
	"strconv"

	"git.code.oa.com/trpc-go/trpc-go/log"
	def "github.com/lovelydayss/goredis/interface"
	"github.com/lovelydayss/goredis/lib/pool"
)

type lineParser func(header []byte, reader *bufio.Reader) *def.Droplet

// Parser 协议命令解析器具体实现
type Parser struct {
	lineParsers map[byte]lineParser
}

// NewParser 初始化
func NewParser() def.Parser {
	p := &Parser{}
	p.lineParsers = map[byte]lineParser{
		'+': p.parseSimpleString,
		'-': p.parseError,
		':': p.parseInt,
		'$': p.parseBulk,
		'*': p.parseMultiBulk,
	}

	return p
}

// ParseStream 连接转换成 stream channel 形式，异步执行
func (p *Parser) ParseStream(reader io.Reader) <-chan *def.Droplet {

	ch := make(chan *def.Droplet)

	pool.Submit(
		func() {
			p.parse(reader, ch)
		})

	return ch
}

// 实际解析，每个解析器单独一个 go routine 处理
// 协程 chan 同步
func (p *Parser) parse(rawReader io.Reader, ch chan<- *def.Droplet) {
	reader := bufio.NewReader(rawReader)
	for {

		// 逐行读数据
		firstLine, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &def.Droplet{
				Reply: def.NewErrReply(err.Error()),
				Err:   err,
			}
			return
		}

		length := len(firstLine)
		if length <= 2 || firstLine[length-1] != '\n' || firstLine[length-2] != '\r' {
			continue
		}

		// 解析请求参数
		firstLine = bytes.TrimSuffix(firstLine, []byte{'\r', '\n'})
		lineParseFunc, ok := p.lineParsers[firstLine[0]]
		if !ok {
			log.Errorf("[parser] invalid line def: %s", firstLine[0])
			continue
		}

		// 发送解析结果
		ch <- lineParseFunc(firstLine, reader)
	}
}

// 解析简单 string 类型
func (p *Parser) parseSimpleString(header []byte, reader *bufio.Reader) *def.Droplet {
	content := header[1:]
	return &def.Droplet{
		Reply: def.NewSimpleStringReply(string(content)),
	}
}

// 解析简单 int 类型
func (p *Parser) parseInt(header []byte, reader *bufio.Reader) *def.Droplet {

	i, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil {
		return &def.Droplet{
			Err:   err,
			Reply: def.NewErrReply(err.Error()),
		}
	}

	return &def.Droplet{
		Reply: def.NewIntReply(i),
	}
}

// 解析错误类型
func (p *Parser) parseError(header []byte, reader *bufio.Reader) *def.Droplet {
	return &def.Droplet{
		Reply: def.NewErrReply(string(header[1:])),
	}
}

// 解析定长 string 类型
func (p *Parser) parseBulk(header []byte, reader *bufio.Reader) *def.Droplet {
	// 解析定长 string
	body, err := p.parseBulkBody(header, reader)
	if err != nil {
		return &def.Droplet{
			Reply: def.NewErrReply(err.Error()),
			Err:   err,
		}
	}
	return &def.Droplet{
		Reply: def.NewBulkReply(body),
	}
}

// 解析定长 string
func (p *Parser) parseBulkBody(header []byte, reader *bufio.Reader) ([]byte, error) {
	// 获取 string 长度
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil {
		return nil, err
	}

	// 长度 + 2，把 CRLF 也考虑在内
	body := make([]byte, strLen+2)
	// 从 reader 中读取对应长度
	if _, err = io.ReadFull(reader, body); err != nil {
		return nil, err
	}
	return body[:len(body)-2], nil
}

// 解析
func (p *Parser) parseMultiBulk(header []byte, reader *bufio.Reader) (droplet *def.Droplet) {
	var _err error
	defer func() {
		if _err != nil {
			droplet = &def.Droplet{
				Reply: def.NewErrReply(_err.Error()),
				Err:   _err,
			}
		}
	}()

	// 获取数组长度
	length, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil {
		_err = err
		return
	}

	if length <= 0 {
		return &def.Droplet{
			Reply: def.NewEmptyMultiBulkReply(),
		}
	}

	lines := make([][]byte, 0, length)
	for i := int64(0); i < length; i++ {
		// 获取每个 bulk 首行
		firstLine, err := reader.ReadBytes('\n')
		if err != nil {
			_err = err
			return
		}

		// bulk 首行格式校验
		length := len(firstLine)
		if length < 4 || firstLine[length-2] != '\r' || firstLine[length-1] != '\n' || firstLine[0] != '$' {
			continue
		}

		// bulk 解析
		bulkBody, err := p.parseBulkBody(firstLine[:length-2], reader)
		if err != nil {
			_err = err
			return
		}

		lines = append(lines, bulkBody)
	}

	return &def.Droplet{
		Reply: def.NewMultiBulkReply(lines),
	}
}
