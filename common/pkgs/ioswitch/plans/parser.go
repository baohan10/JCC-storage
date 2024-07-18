package plans

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type FromToParser interface {
	Parse(ft FromTo, blder *PlanBuilder) error
}

type DefaultParser struct {
	EC *cdssdk.ECRedundancy
}

func (p *DefaultParser) Parse(ft FromTo, blder *PlanBuilder) error {

}
