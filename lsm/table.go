// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lsm

import (
	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"os"
)

type table struct {

	// ss 封装了SSTable 支持的文件操作 支持SStable 的内存封装
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
}

// builder为nil 说明是加载原来已有的内容
func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {

	// 打开 SSTable
	ss := file.OpenSSTable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lm.opt.SSTableMaxSz),
	})

	// 初始化 table
	t := &table{
		ss:  ss,
		lm:  lm,
		fid: utils.FID(tableName),
	}

	// 如果builder 不是nil 说明是要刷盘
	if builder != nil {
		if err := builder.flush(ss); err != nil {
			utils.Err(err)
			return nil
		}
	}

	//解析磁盘的字符数组
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	return t
}

// Serach 从table中查找key
func (t *table) Serach(key []byte, maxVs *uint64) (entry *codec.Entry, err error) {

	return nil, utils.ErrKeyNotFound
}
