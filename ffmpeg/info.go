package ffmpeg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"

	"github.com/golang/glog"
)

// VideoInfo ...
type VideoInfo struct {
	Format   string
	Codec    string
	Width    int
	Height   int
	Duration float64
}

// ReadVideoInfo ...
func ReadVideoInfo(videoBuffer io.Reader) (info VideoInfo, err error) {
	type ffprobeJSON struct {
		Streams []struct {
			CodecName   string `json:"codec_name"`
			Width       int    `json:"width"`
			Height      int    `json:"height"`
			DurationStr string `json:"duration"`
		} `json:"streams"`
		Format struct {
			Name string `json:"format_name"`
		}
	}

	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "format=format_name:stream=width,height,duration,codec_name",
		"-print_format", "json",
		"-",
	)
	cmd.Stdin = videoBuffer
	cmd.Stderr = os.Stderr

	out, err := cmd.Output()
	if err != nil {
		glog.Error(err)
		return
	}

	var result ffprobeJSON
	if err = json.NewDecoder(bytes.NewBuffer(out)).Decode(&result); err != nil {
		glog.Error(err)
		return
	}
	if len(result.Streams) == 0 {
		err = fmt.Errorf("empty stream")
		glog.Error(err)
		return
	}
	stream := result.Streams[0]

	duration, err := strconv.ParseFloat(stream.DurationStr, 64)
	if err != nil {
		glog.Error(err)
		return
	}

	info = VideoInfo{
		Format:   result.Format.Name,
		Codec:    stream.CodecName,
		Duration: duration,
		Width:    stream.Width,
		Height:   stream.Height,
	}

	return
}
