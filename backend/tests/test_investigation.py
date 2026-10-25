import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
import main


class PlanAndEvidenceTests(unittest.TestCase):
    def test_date_normalization_is_program_owned(self):
        context = main.date_context('明天去上海自然博物馆')
        self.assertEqual(context['timezone'], 'Asia/Shanghai')
        self.assertIsNotNone(context['targetDate'])

    def test_invalid_source_url_is_dropped_but_unknown_is_kept(self):
        self.assertIsNone(main.normalize_hit({'Url': 'javascript:alert(1)'}))
        unknown = main.normalize_hit({'Url': 'https://python.langchain.com/docs', 'Title': 'Docs', 'Passage': 'Runnable API'})
        self.assertEqual(unknown['sourceType'], 'unknown')

    def test_evidence_requires_real_source_and_exact_excerpt(self):
        state={'plan':{'subquestions':[{'id':'Q1','checkpoints':[{'id':'C1','description':'功能'}]}]},'sources':[{'id':'S1','snippet':'官方说明支持导出','content':None}], 'evidence':[]}
        good=main.valid_evidence([{'sourceId':'S1','checkpointIds':['C1'],'statement':'官方说明支持导出功能','relation':'supports','excerpt':'官方说明支持导出','excerptFrom':'snippet'}],state)
        bad=main.valid_evidence([{'sourceId':'S9','checkpointIds':['C1'],'statement':'不存在','relation':'supports','excerpt':'不存在的引文'}],state)
        self.assertEqual(len(good),1); self.assertEqual(bad,[])

    def test_review_fills_missing_checkpoints_as_pending(self):
        state={'plan':{'subquestions':[{'id':'Q1','checkpoints':[{'id':'C1','description':'开放'},{'id':'C2','description':'票价'}]}]},'evidence':[]}
        result=main.statuses_from_review([{'checkpoint_id':'C1','status':'supported','evidence_ids':[],'reason':'有来源'}],state)
        # C1：LLM 声称支持但没有可引用证据，降级为 pending；C2：LLM 漏判，兜底 pending
        self.assertEqual([x['status'] for x in result],['pending','pending'])

    def test_review_upgrades_when_independent_sources_exist(self):
        state={'plan':{'subquestions':[{'id':'Q1','checkpoints':[{'id':'C1','description':'开放'},{'id':'C2','description':'票价'}]}]},'evidence':[
            {'id':'E1','checkpointIds':['C1'],'sourceId':'S1','relation':'supports','temporalFit':'matched'},
            {'id':'E2','checkpointIds':['C1'],'sourceId':'S2','relation':'supports','temporalFit':'matched'},
        ]}
        # LLM 完全漏判，但 C1 有两个独立来源支持，应升级为 supported
        result=main.statuses_from_review([],state)
        self.assertEqual({x['checkpoint_id']:x['status'] for x in result},{'C1':'supported','C2':'pending'})


class FlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_questions_are_covered_and_requests_are_budgeted(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(main,'DB',Path(folder)/'db'), patch.object(main,'WSA_KEY','x'), patch.object(main,'DEEPSEEK_KEY','x'):
            main.init_db()
            plan={'out_of_scope':False,'requires_clarification':False,'normalized_question':'测试','question_type':'information_lookup','subquestions':[{'id':'Q1','question':'开放？','checkpoints':[{'id':'C1','description':'规则'}]},{'id':'Q2','question':'票价？','checkpoints':[{'id':'C2','description':'价格'}]}]}
            jobs={'jobs':[{'checkpoint_ids':['C1'],'query':'对象 开放 官方','purpose':'规则','reason':''},{'checkpoint_ids':['C2'],'query':'对象 票价 官方','purpose':'价格','reason':''}]}
            hit=main.normalize_hit({'Url':'https://example.org/a','Title':'说明','Passage':'对象开放，普通票价为10元'})
            extracted={'items':[]} # extractor schema is an array; invalid output must not create evidence
            review=[{'checkpoint_id':'C1','status':'insufficient','evidence_ids':[],'reason':'摘要不足','missing_information':'全文'},{'checkpoint_id':'C2','status':'insufficient','evidence_ids':[],'reason':'摘要不足','missing_information':'全文'}]
            with patch.object(main,'llm',AsyncMock(side_effect=[plan,jobs,review,jobs,review,jobs,review])), patch.object(main,'search_yuanbao',AsyncMock(return_value=([hit],None,1))):
                report=await main.investigate_claim('测试一个公共问题')
            self.assertEqual(len(report['subquestions']),2)
            self.assertEqual({c['checkpoint_id'] for c in report['checkpoints']},{'C1','C2'})
            self.assertLessEqual(report['search_count'],main.MAX_REQUESTS)
            self.assertEqual(report['overallStatus'],'partial')

    async def test_scope_and_tool_failure_never_fake_success(self):
        with patch.object(main,'WSA_KEY','x'), patch.object(main,'DEEPSEEK_KEY','x'), patch.object(main,'llm',AsyncMock(side_effect=RuntimeError('模型故障'))):
            with self.assertRaises(main.HTTPException) as caught:
                await main.investigate_claim('查一个公共问题')
            self.assertEqual(caught.exception.status_code,502)


if __name__=='__main__': unittest.main()
