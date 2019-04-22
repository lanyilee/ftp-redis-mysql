package core

type TPeUser struct {
	Phone int64 `xorm:"not null pk BigInt(11)"`
}

func NewPeUser() *TPeUser{
	return &TPeUser{}
}

type TPeUpdatestatus struct {
	Id int `xorm:"not null pk int(5)"`
	Status int `xorm:"int(5)"`
}


type Phone struct {
	Mobile string
}