package orm

import (
	"time"
)

type Artifact struct {
	Author string `gorm:"primaryKey;size:255;not null" json:"author"`
	Name   string `gorm:"primaryKey;size:255;not null" json:"name"`
	Hash   string `gorm:"primaryKey;size:64;not null"  json:"hash"`

	CreatedAt  time.Time `gorm:"not null;default:CURRENT_TIMESTAMP" json:"createdAt"`
	PullsCount int64     `gorm:"default:0"                          json:"pullsCount"`

	// Reverse relationship to tags with cascading deletion
	Tags []Tag `gorm:"foreignKey:Author,Name,Hash;references:Author,Name,Hash;constraint:OnDelete:CASCADE" json:"tags,omitempty"`
}

type Tag struct {
	// Composite primary key that also serves as foreign key to Artifact
	Author  string `gorm:"primaryKey;size:255;not null" json:"author"`
	Name    string `gorm:"primaryKey;size:255;not null" json:"name"`
	TagName string `gorm:"primaryKey;size:255;not null" json:"tagName"`
	Hash    string `gorm:"size:64;not null"             json:"hash"`
}
