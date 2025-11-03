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
}

type Tag struct {
	Source  string `gorm:"primaryKey;size:255;not null" json:"source"`
	Author  string `gorm:"primaryKey;size:255;not null" json:"author"`
	Name    string `gorm:"primaryKey;size:255;not null" json:"name"`
	Hash    string `gorm:"primaryKey;size:64;not null"  json:"hash"`
	TagName string `gorm:"size:255;not null" json:"tagName"`
}
