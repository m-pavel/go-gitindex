package index

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/blevesearch/bleve"

	"gopkg.in/libgit2/git2go.v26"
)

type GitIndex struct {
	repo     *git.Repository
	gitdir   string
	indexdir string
	idprefix string
	index    bleve.Index
}

type GitEntry struct {
	Id     string
	Msg    string
	Author string
}

func fromCommit(commit *git.Commit) *GitEntry {
	return &GitEntry{Id: commit.Id().String(), Msg: commit.Message(), Author: commit.Author().Name}
}
func fromMap(m map[string]interface{}) *GitEntry {
	return &GitEntry{Id: m["Id"].(string), Msg: m["Msg"].(string), Author: m["Author"].(string)}
}

func New(url string, indexdir string, idprefix ...string) (*GitIndex, error) {
	gi := GitIndex{indexdir: indexdir}
	if len(idprefix) == 1 {
		gi.idprefix = idprefix[0]
	}
	var err error

	if gi.gitdir, err = ioutil.TempDir(os.TempDir(), "gm"); err != nil {
		return nil, err
	}
	if gi.repo, err = git.Clone(url, gi.gitdir, &git.CloneOptions{}); err != nil {
		return nil, err
	}
	return &gi, nil
}
func (gi *GitIndex) Close() error {
	return os.RemoveAll(gi.gitdir)
}

func (gi *GitIndex) Index() error {
	var err error

	fi, err := os.Stat(gi.indexdir)
	if err == nil && fi.IsDir() {
		if gi.index, err = bleve.Open(gi.indexdir); err != nil {
			return err
		}
	} else {
		mapping := bleve.NewIndexMapping()
		if gi.index, err = bleve.New(gi.indexdir, mapping); err != nil {
			return err
		}
	}

	bi, err := gi.repo.NewBranchIterator(git.BranchAll)
	if err != nil {
		return err
	}
	bi.ForEach(func(b *git.Branch, bt git.BranchType) error {
		r, err := gi.repo.Head()
		if err != nil {
			return err
		}
		c, err := gi.repo.LookupCommit(r.Target())
		if err != nil {
			return err
		}
		gi.processCommit(c)
		return nil
	})
	return nil
}

func (gi *GitIndex) Search(msg string) []*GitEntry {
	var batch uint64
	batch = 30
	//query := bleve.NewMatchAllQuery()
	query := bleve.NewQueryStringQuery(msg)
	req := bleve.NewSearchRequestOptions(query, int(batch), 0, true)
	req.Fields = []string{"*"}
	searchResult, _ := gi.index.Search(req)

	res := make([]*GitEntry, searchResult.Total)
	residx := 0
	processResult(searchResult, res, &residx)

	start := batch
	var bi uint64
	for bi = 0; bi < searchResult.Total/batch+1; bi++ {
		req := bleve.NewSearchRequestOptions(query, int(batch), int(start), true)
		req.Fields = []string{"*"}
		searchResult, _ := gi.index.Search(req)
		processResult(searchResult, res, &residx)
		start += batch
	}
	return res
}

func processResult(searchResult *bleve.SearchResult, res []*GitEntry, residx *int) {
	for _, h := range searchResult.Hits {
		res[*residx] = fromMap(h.Fields)
		*residx++
	}
}

func (gi *GitIndex) processCommit(c *git.Commit) {
	//fmt.Printf("%s %s %s\n", c.Id().String(), c.Author(), c.Message())
	gi.index.Index(fmt.Sprintf("%s-%s", gi.idprefix, c.Id().String()), fromCommit(c))
	for i := 0; i < int(c.ParentCount()); i++ {
		cp := c.Parent(uint(i))
		gi.processCommit(cp)
	}
}
