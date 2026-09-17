package config

import (
	"io"
	"os"
	"syscall"
)

const maxFileBytes = 1 << 20

type fieldError struct{ field, code, message string }

func (e *fieldError) Error() string { return e.message }

func invalidField(field, code, message string) error {
	return &fieldError{field: field, code: code, message: message}
}

// ReadReferencedFile reads at most 1 MiB from a regular configuration/Secret
// file. Kubernetes Secret symlinks are followed. Nonblocking open followed by
// fstat avoids a FIFO race between checking and opening the path.
func ReadReferencedFile(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, invalidField("", "file_unreadable", "cannot open configuration file")
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, invalidField("", "file_unreadable", "cannot inspect configuration file")
	}
	if !info.Mode().IsRegular() {
		return nil, invalidField("", "file_not_regular", "configuration file must resolve to a regular file")
	}
	if info.Size() > maxFileBytes {
		return nil, invalidField("", "file_too_large", "configuration file exceeds 1 MiB bound")
	}
	b, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	if err != nil {
		return nil, invalidField("", "file_unreadable", "cannot read configuration file")
	}
	if len(b) > maxFileBytes {
		return nil, invalidField("", "file_too_large", "configuration file exceeds 1 MiB bound")
	}
	return b, nil
}
