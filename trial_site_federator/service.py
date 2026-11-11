from __future__ import annotations
import json
import uuid
from pathlib import Path
from typing import Any, Mapping
from trial_site_federator.runtime import DomainError, ProcessingContext, SQLiteLedger, fingerprint, normalize_text
from trial_site_federator.adapters.site_feeds import build_processor as site_feeds_factory
from trial_site_federator.protocols.identity import build_processor as identity_factory
from trial_site_federator.lineage.amendments import build_processor as amendments_factory
from trial_site_federator.eligibility.summary import build_processor as summary_factory
from trial_site_federator.recruitment.status import build_processor as status_factory
from trial_site_federator.dedup.record_linkage import build_processor as record_linkage_factory
from trial_site_federator.monitoring.freshness import build_processor as freshness_factory
from trial_site_federator.catalog.snapshot import build_processor as snapshot_factory

PROCESSOR_FACTORIES = [site_feeds_factory, identity_factory, amendments_factory, summary_factory, status_factory, record_linkage_factory, freshness_factory, snapshot_factory]

class ApplicationService:
    def __init__(self, database: str | Path):
        self.ledger=SQLiteLedger(database)
        self.processors=[factory() for factory in PROCESSOR_FACTORIES]

    def validate_input(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping): raise DomainError("record must be object")
        result=dict(record)
        result["case_id"]=normalize_text(result.get("case_id"),"case_id",96)
        if not result["case_id"]: raise DomainError("case_id is required")
        result.setdefault("subject_id",result["case_id"])
        result.setdefault("category","screening")
        result.setdefault("region","synthetic-region")
        result.setdefault("event_date","2026-11-11")
        result.setdefault("status","new")
        result.setdefault("count",1)
        result.setdefault("total",1)
        result.setdefault("attributes",{"source":"synthetic"})
        result.setdefault("tags",["offline","fixture"])
        result.setdefault("source_system","local-fixture")
        result.setdefault("revision",0)
        return result

    def execute(self, record: Mapping[str, Any], request_key: str | None=None) -> dict[str, Any]:
        normalized=self.validate_input(record)
        normalized=self.architecture_preflight(normalized)
        key=request_key or fingerprint(normalized)
        recalled=self.ledger.recalled(key)
        if recalled is not None: return recalled | {"replayed":True}
        ctx=ProcessingContext(uuid.uuid4().hex,normalized["case_id"],normalized)
        ctx.emit("received","run_started",project='trial_site_federator')
        try:
            for processor in self.architecture_plan(ctx):
                processor.process(ctx)
                self.ledger.save(ctx)
        except Exception as exc:
            self.ledger.dead_letter(ctx,ctx.state,str(exc),normalized)
            raise
        self.architecture_finalize(ctx)
        ctx.advance("completed")
        self.ledger.save(ctx)
        response={"project":'trial_site_federator',"run_id":ctx.run_id,"case_id":ctx.case_id,"state":ctx.state,
                  "revision":ctx.revision,"flags":ctx.flags,"metrics":ctx.metrics,"evidence_count":len(ctx.evidence),
                  "event_count":len(ctx.events),"payload":ctx.payload,"replayed":False}
        self.ledger.remember(key,ctx.run_id,response)
        return response

    def architecture_preflight(self, record):
        record=dict(record)
        site=record.get("attributes",{}).get("site",record.get("region","unknown-site"))
        protocol=record.get("attributes",{}).get("protocol",record.get("case_id"))
        record["federation"]={"site":site,"protocol":protocol,"lineage":[],"freshness_days":0,"duplicates":[]}
        return record

    def architecture_plan(self, ctx):
        plan=[]
        for processor in self.processors:
            ctx.payload["federation"]["lineage"].append({"stage":processor.stage,"source":ctx.payload["federation"]["site"]})
            plan.append(processor)
        return plan

    def architecture_finalize(self, ctx):
        fed=ctx.payload["federation"]
        fed["catalog_key"]=fingerprint({"protocol":fed["protocol"],"site":fed["site"]})[:20]
        fed["recruitment_state"]=ctx.payload.get("status","unknown")
        fed["stale"]=fed["freshness_days"]>30
        ctx.metrics["lineage_depth"]=len(fed["lineage"])
        ctx.emit("federation_catalog","protocol_published",catalog_key=fed["catalog_key"])

    def execute_many(self, records: list[Mapping[str, Any]]) -> dict[str, Any]:
        outputs=[]; failures=[]
        for index, record in enumerate(records):
            try: outputs.append(self.execute(record,request_key=f"batch:{index}:{fingerprint(record)}"))
            except Exception as exc: failures.append({"index":index,"error":str(exc)})
        return {"accepted":outputs,"failures":failures,"accepted_count":len(outputs),"failure_count":len(failures)}

def run_fixture(fixture: str | Path, database: str | Path) -> dict[str, Any]:
    data=json.loads(Path(fixture).read_text(encoding="utf-8"))
    service=ApplicationService(database)
    if isinstance(data,list): return service.execute_many(data)
    return service.execute(data)
