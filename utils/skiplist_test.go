package utils

import (
	"github.com/stretchr/testify/assert"
	"github.com/zhangx1n/MyKV/utils/codec"
	"testing"
)

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList()

	// Put & Get
	entry1 := codec.NewEntry([]byte("key1"), []byte("value1"))
	assert.Nil(t, list.Add(entry1))
	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)

	entry2 := codec.NewEntry([]byte("Key2"), []byte("Val2"))
	list.Add(entry2)
	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)

	//Get a not exist entry
	assert.Nil(t, list.Search([]byte("noexist")))

	//Delete a entry
	list.Remove([]byte("Key2"))
	assert.Nil(t, list.Search(entry2.Key))

	//Update a entry
	entry2_new := codec.NewEntry([]byte("Key1"), []byte("Val1+1"))
	assert.Nil(t, list.Add(entry2_new))
	assert.Equal(t, entry2_new.Value, list.Search(entry2_new.Key).Value)
}
