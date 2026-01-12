package storage

const PAGE_SIZE = 4096

type (
	PageID uint64
	Page   [PAGE_SIZE]byte
)
