package entries


import(
	"github.com/MalikL2005/Go_DB/btree"
)


func InsertToBtree (root **btree.Node_t, key int, val int) error {
    err := btree.Insert(root, *root, btree.Entry_t{Key:key, Val:val})
    if err != nil {
        return err
    }
    return nil
}
