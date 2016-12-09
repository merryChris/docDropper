package core

import (
	"fmt"

	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
)

//NOTE: (zacky, 2016.DEC.7st) HOW TO NEST IT INTO `News`?
type BriefNews struct {
	Id     uint32 `json:"id"`
	Source string `json:"source"`
	Title  string `json:"title"`
}

type News struct {
	Id      uint32 `orm:"column(nId)"`
	Source  string `orm:"column(link)"`
	Title   string `orm:"column(title)"`
	Summary string `orm:"column(abstracts)"`
	Content string `orm:"column(content)"`
}

type NewsList struct {
	Units []News
}

func InitOrm(username string,
	password string,
	host string,
	port string,
	dbname string,
	maxIdleConnections int,
	maxOpenConnections int) error {
	if err := orm.RegisterDataBase("default", "mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&loc=Local", username, password, host, port, dbname)); err != nil {
		return err
	}
	orm.SetMaxIdleConns("default", maxIdleConnections)
	orm.SetMaxOpenConns("default", maxOpenConnections)

	return nil
}

func (this *NewsList) GetNews(limit uint32) error {
	o := orm.NewOrm()
	sql := fmt.Sprintf("SELECT nId, link, title, abstracts, content FROM `news` LIMIT %d", limit)
	fmt.Printf("sql: %s\n", sql)
	if _, err := o.Raw(sql).QueryRows(&this.Units); err != nil {
		return err
	}
	return nil
}
