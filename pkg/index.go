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
	Id          string `json:"id"`
	Msg         string `json:"message"`
	Author      string `json:"author_name"`
	AuthorEmail string `json:"author_email"`
}

func fromCommit(commit *git.Commit) *GitEntry {
	return &GitEntry{Id: commit.Id().String(), Msg: commit.Message(), Author: commit.Author().Name, AuthorEmail: commit.Author().Email}
}
func fromMap(m map[string]interface{}) *GitEntry {
	return &GitEntry{Id: m["id"].(string), Msg: m["message"].(string), Author: m["author_name"].(string), AuthorEmail: m["author_email"].(string)}
}

func NewLocal(repo *git.Repository, indexdir string, idprefix string) (*GitIndex, error) {
	gi := GitIndex{indexdir: indexdir, repo: repo, idprefix: idprefix}
	return &gi, nil
}

func New(url string, indexdir string, idprefix ...string) (*GitIndex, error) {
	var err error
	var dir string
	var repo *git.Repository
	if dir, err = ioutil.TempDir(os.TempDir(), "gm"); err != nil {
		return nil, err
	}
	if repo, err = git.Clone(url, dir, &git.CloneOptions{}); err != nil {
		return nil, err
	}
	prefixparam := ""
	if len(idprefix) == 1 {
		prefixparam = idprefix[0]
	}
	if re, err := NewLocal(repo, indexdir, prefixparam); err != nil {
		return nil, err
	} else {
		re.gitdir = dir
		return re, nil
	}
}

func (gi *GitIndex) Close() error {
	if gi.gitdir != "" {
		if err := os.RemoveAll(gi.gitdir); err != nil {
			return err
		}
	}
	return gi.index.Close()
}

func (gi *GitIndex) Index() error {
	var err error
	if gi.index, err = getIndex(gi.indexdir); err != nil {
		return err
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

func getIndex(dir string) (bleve.Index, error) {
	var err error

	fi, err := os.Stat(dir)
	if err == nil && fi.IsDir() {
		return bleve.Open(dir)
	} else {
		mapping := bleve.NewIndexMapping()
		return bleve.New(dir, mapping)
	}
}
func Search(indexdir, msg string, minScore float64) ([]*GitEntry, error) {
	var batch uint64
	batch = 30
	//query := bleve.NewMatchAllQuery()
	query := bleve.NewQueryStringQuery(msg)
	req := bleve.NewSearchRequestOptions(query, int(batch), 0, true)
	req.Fields = []string{"*"}
	index, err := getIndex(indexdir)
	if err != nil {
		return nil, err
	}
	defer index.Close()
	searchResult, err := index.Search(req)
	if err != nil {
		return nil, err
	}
	res := make([]*GitEntry, searchResult.Total)
	residx := 0
	processResult(searchResult, res, &residx, minScore)

	start := batch
	var bi uint64
	for bi = 0; bi < searchResult.Total/batch+1; bi++ {
		req := bleve.NewSearchRequestOptions(query, int(batch), int(start), true)
		req.Fields = []string{"*"}
		searchResult, _ := index.Search(req)
		processResult(searchResult, res, &residx, minScore)
		start += batch
	}
	return res[:residx], nil
}

func processResult(searchResult *bleve.SearchResult, res []*GitEntry, residx *int, minScore float64) {
	for _, h := range searchResult.Hits {
		if h.Score >= minScore {
			res[*residx] = fromMap(h.Fields)
			*residx++
		}
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
