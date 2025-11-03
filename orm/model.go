package orm

import (
	"time"
)

type Artifact struct {
	Source string `gorm:"primaryKey;size:255;not null" json:"source"`
	Author string `gorm:"primaryKey;size:255;not null" json:"author"`
	Name   string `gorm:"primaryKey;size:255;not null" json:"name"`
	Hash   string `gorm:"primaryKey;size:64;not null"  json:"hash"`

	CreatedAt  time.Time `gorm:"not null;default:CURRENT_TIMESTAMP" json:"createdAt"`
	PullsCount int64     `gorm:"default:0"                          json:"pullsCount"`

	Tags []Tag `gorm:"foreignKey:Source,Author,Name,Hash;references:Source,Author,Name,Hash" json:"tags,omitempty"`
}

type Tag struct {
	Source string `gorm:"primaryKey;size:255;not null" json:"source"`
	Author string `gorm:"primaryKey;size:255;not null" json:"author"`
	Name   string `gorm:"primaryKey;size:255;not null" json:"name"`
	Tag    string `gorm:"primaryKey;size:255;not null" json:"tag"`

	Hash string `gorm:"size:64;not null;index" json:"hash"`

	Artifact *Artifact `gorm:"foreignKey:Source,Author,Name,Hash;references:Source,Author,Name,Hash" json:"artifact,omitempty"`
}
