package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFullName(t *testing.T) {
	tags := Tags{"b": "bbb", "p": "ppp", "a": "aaa"}
	dp := Datapoint{Name: "test.metric", Tags: tags}
	assert.Equal(t, "test.metric;a=aaa;b=bbb;p=ppp", dp.FullName())

}

func TestDirectory(t *testing.T) {
	dp := Datapoint{Name: "test.directory.metric", Tags: Tags{}}
	dir, _ := dp.Directory()
	assert.Equal(t, "test.directory", dir)

	dp = Datapoint{Name: "", Tags: Tags{}}
	_, err := dp.Directory()
	assert.Error(t, err)
}
