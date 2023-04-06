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

package file

import (
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
	"github.com/pkg/errors"
	"io"
	"os"
	"sync"
	"time"
)

// TODO LAB 在这里实现 sst 文件操作

//SSTbale 的内存视图

type SSTable struct {
	lock *sync.RWMutex
	//mmap 文件句柄
	f      *MmapFile
	maxKey []byte
	minKey []byte

	//索引段
	idxTables *pb.TableIndex
	idxLen    int
	idxStart  int

	hasBloomfilter bool
	//SStable文件句柄
	fid uint32
	//创建时间
	createdAt time.Time
}

// 文件操作 根据配置文件打开一个SSTable 文件

//打开一个 SSTable 文件 调用mmap 封装好的打开文件接口

func OpenSSTable(opt *Options) *SSTable {

	omf, err := OpenMmapFile(opt.FileName, os.O_RDWR|os.O_CREATE, opt.MaxSz)
	utils.Err(err)
	return &SSTable{
		f:    omf,
		fid:  opt.FID,
		lock: &sync.RWMutex{},
	}
}

//初始化操作

func (ss *SSTable) Init() error {

	var ko *pb.BlockOffset
	var err error

	if ko, err = ss.initTable(); err != nil {
		return err
	}

	//todo： SSTable 最大key 和 最小值key 是 怎么取的

	// 拿到 block 中的最大key 和 最小值
	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey

	blockLen := len(ss.idxTables.Offsets)
	ko = ss.idxTables.Offsets[blockLen-1]
	keyBytes = ko.GetKey()
	maxKey := make([]byte, len(keyBytes))
	copy(maxKey, keyBytes)
	ss.maxKey = maxKey
	return nil
}

func (ss *SSTable) initTable() (bo *pb.BlockOffset, err error) {
	//按照规定的SSTable 内存视图 解码
	readPos := len(ss.f.Data)
	readPos -= 4

	buf := ss.readCheckError(readPos, 4)

	checksumLen := int(codec.BytesToU32(buf))

	if checksumLen < 0 {
		return nil, errors.New("checksum len less than zero")
	}

	readPos -= checksumLen

	expectedChk := ss.readCheckError(readPos, checksumLen)

	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	ss.idxLen = int(codec.BytesToU32(buf))

	readPos -= ss.idxLen
	//索引段 这里crc32 校验和校验的是索引段
	ss.idxStart = readPos
	data := ss.readCheckError(readPos, ss.idxLen)
	if err := utils.VerifyChecksum(data, expectedChk); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", ss.f.Fd.Name())
	}

	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}

	ss.idxTables = indexTable
	ss.hasBloomfilter = len(indexTable.BloomFilter) > 0

	//这里返回的是 第一个block块的地址 BlockOffset
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}

	return nil, errors.New("read index fail,offset is nil")
}

func (ss *SSTable) FID() uint32 {
	return ss.fid
}

func (ss *SSTable) read(offset int, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		if len(ss.f.Data[offset:]) < sz {
			return nil, io.EOF
		}

		return ss.f.Data[offset : offset+sz], nil
	}
	// 防止mmap 没有映射数据 当然机率很小
	res := make([]byte, sz)
	_, err := ss.f.Fd.ReadAt(res, int64(offset))
	return res, err
}

func (ss *SSTable) readCheckError(offset int, sz int) (buf []byte) {

	buf, err := ss.read(offset, sz)
	if err != nil {
		utils.Panic(err)
	}
	return buf
}
