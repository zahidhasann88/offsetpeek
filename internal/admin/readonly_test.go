package admin

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// writeMethods are kadm/kgo calls that mutate broker state; real.go must not use any.
var writeMethods = map[string]bool{
	"CommitOffsets": true, "CommitAllOffsets": true, "CommitRecords": true,
	"CommitUncommittedOffsets": true, "CommitMarkedOffsets": true,
	"DeleteOffsets": true, "DeleteGroup": true, "DeleteGroups": true,
	"LeaveGroup": true, "AlterPartitionAssignments": true,
	"CreateTopic": true, "CreateTopics": true, "DeleteTopic": true, "DeleteTopics": true,
	"DeleteRecords": true, "CreatePartitions": true,
	"AlterTopicConfigs": true, "AlterBrokerConfigs": true,
	"Produce": true, "ProduceSync": true, "TryProduce": true,
	"ConsumerGroup": true, // joining a group could commit offsets
}

func TestRealClientMakesNoWriteCalls(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "real.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if ok && writeMethods[sel.Sel.Name] {
			t.Errorf("%s: real.go references write API %q", fset.Position(sel.Pos()), sel.Sel.Name)
		}
		return true
	})
}
