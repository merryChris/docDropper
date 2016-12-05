package core

import (
	"fmt"

	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
)

type News struct {
	Source  string
	Title   string
	Summary string
	Content string
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

func (this *NewsList) GetWangjiaNews(max int) error {
	o := orm.NewOrm()
	sql := fmt.Sprintf("SELECT source, title, summary, content FROM `wangjia_news` LIMIT %d", max)
	fmt.Printf("sql:%s\n", sql)
	if _, err := o.Raw(sql).QueryRows(&this.Units); err != nil {
		return err
	}
	return nil
}
