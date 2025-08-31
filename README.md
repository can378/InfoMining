# InfoMining
" 개발에 관련된 키워드를 받으면 그것과 관련된 정보를 긁어와 주는 miner "

---
### 긁어올 데이터 장소들
Google - 정보 많음

TechCrunch – 스타트업·AI·신기술 제품 출시 뉴스에 강함.

The Verge – 소비자 기술, AI 트렌드, 빅테크 제품 소식.

Wired – 과학+기술+사회적 맥락 해설.

Ars Technica – 개발자 친화적인 심층 기술 기사 많음.

MIT Technology Review – “이 달의 혁신 기술”류 콘텐츠.

VentureBeat (AI/Deep Learning) – AI 산업 뉴스와 기업 적용 사례.

Bloomberg Technology / Reuters Tech – 글로벌 빅테크 비즈니스 흐름.

Hacker News (news.ycombinator.com) – 실리콘밸리 개발자/스타트업 커뮤니티, 링크 기반이지만 속보성·깊이 모두 있음.

Reddit r/MachineLearning / r/Artificial / r/technology – 커뮤니티 기반으로 신기한 논문, 도구, 제품 소식이 빠름.

Youtube - #Two Minute Papers #Computerphile

---
### .env 설정
```
YOUTUBE_API_KEY=
GOOGLE_API_KEY=
GOOGLE_CX=

CRAWL_CONCURRENCY=12
CRAWL_TIMEOUT_MS=30000
CRAWL_RENDER_JS=true
CRAWL_CACHE_DIR=.c4ai_cache

# 랭킹/선정 옵션
FINAL_N=40
MIN_CONTENT_CHARS=800
PROFILE_TEXT=나는 ~~ 관련 글을 좋아한다.


```
GOOGLE_API_KEY= https://console.cloud.google.com/welcome?
에서 Create credentials누르고 API Key 선택해서 만들면된다.
GOOGLE_CX= https://programmablesearchengine.google.com/about/ 에서 Get Started누르고 search engines를 Add하면된다. 

+) search engines만들면 아래와 같은 것이 뜨는데 cx=다음이 키값이다.
```
<script async src="https://cse.google.com/cse.js?cx=키값">
</script>
<div class="gcse-search"></div>
```

+) 제대로 발급 받았는지 테스트
```
curl "https://www.googleapis.com/customsearch/v1?key=<API키값>&cx=<CX값>&q=AI"
```

---
### START 
가상환경 설정, 의존성 설치
```
python -m venv .venv
.venv\Scripts\activate

pip install feedparser httpx python-dateutil
pip install -r requirements.txt
pip freeze > requirements.txt
```


---
# EXECUTE
```
python app/fetch_google.py
python app/fetch_rss.py 
--> data 폴더아래에 결과 나옴

python app/crawl_extract.py --> crawl_result 폴더 아래에 결과 출력

python app/curate_results.py --> results폴더아래에 결과 출력
```


---
### CRAWL TEST
app/crawl_test.py 로 crawl4ai 테스트 할 수 있다.

### fetch_youtube 실행 명령어 예시
python fetch_youtube.py global --query "AI product launch" --published-after "2025-08-01T00:00:00Z" --limit 150 --out data/global_ai_launch.jsonl --jsonl
