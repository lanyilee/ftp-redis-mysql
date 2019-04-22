package core

import "io"

type FakeCloseReadCloser struct {
	io.ReadCloser
}

func (w *FakeCloseReadCloser) Close() error{
	return nil
}

func (w *FakeCloseReadCloser) RealClose() error{
	if w.ReadCloser ==nil{
		return nil
	}
	return w.ReadCloser.Close()
}