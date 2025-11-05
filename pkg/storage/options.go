package storage

// WriterAttrs holds optional attributes for storage writers
type WriterAttrs struct {
	ContentType     string
	ContentEncoding string
	Metadata        map[string]string
}

// WriterOption is a function that modifies WriterAttrs
type WriterOption func(*WriterAttrs)

// WithContentType sets the content type for the storage object
func WithContentType(contentType string) WriterOption {
	return func(attrs *WriterAttrs) {
		attrs.ContentType = contentType
	}
}

// WithContentEncoding sets the content encoding for the storage object
func WithContentEncoding(encoding string) WriterOption {
	return func(attrs *WriterAttrs) {
		attrs.ContentEncoding = encoding
	}
}

// WithMetadata sets custom metadata for the storage object
func WithMetadata(key, value string) WriterOption {
	return func(attrs *WriterAttrs) {
		if attrs.Metadata == nil {
			attrs.Metadata = make(map[string]string)
		}
		attrs.Metadata[key] = value
	}
}
