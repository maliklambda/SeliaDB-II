package entries


import(
	"github.com/MalikL2005/SeliaDB-II/btree"
)


func InsertToBtree (root **btree.Node_t, key uint32, val uint32) error {
    if root == nil {
        newRoot := &btree.Node_t{}
        root = &(newRoot)
    }
    err := btree.Insert(root, *root, btree.Entry_t{Key:key, Value:val})
    if err != nil {
        return err
    }
    return nil
}
