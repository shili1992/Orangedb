package transaction

//import "github.com/hnakamur/ltsvlog"
//
//func withTransction(db *leveldb.DB, fn func(tx *leveldb.Transaction) error) error {
//tx, err := db.OpenTransaction()
//if err != nil {
//ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to open transaction"},
//ltsvlog.LV{"err", err})
//return err
//}
//
//err = fn(tx)
//if err != nil {
//tx.Discard()
//return err
//}
//
//err = tx.Commit()
//if err != nil {
//ltsvlog.Logger.ErrorWithStack(ltsvlog.LV{"msg", "failed to commit transaction"},
//ltsvlog.LV{"err", err})
//return err
//}
//return nil
}