# 아이의 영화 영화 데이터 호출&저장 자동화

## 개요
이 repo는 [아이의 영화](https://github.com/harmlessman/ChildMovie)의 업데이트 기능에 사용하는 Firestore에 최신 영화 데이터를 저장하는 기능을 수행한다.

영화 데이터는 공공데이터 API의 영화 정보와 ORS의 영화 정보 중 서술적내용기술을 의미한다.

main.py 실행 시 공공데이터 API를 호출하여 영화 정보를 가져오고, ORS의 영화 정보 중 서술적내용기술을 크롤링 하여 가져온다.
그 후, 당일 날짜를 collectino id 로 하여 영화 정보들을 Firestore에 저장한다. (ex - '20231118')

api key나 firebase key 등은 올리지 않음

## 사용법
* GCP의 Cloud Build로 컨테이너 이미지를 빌드한다.

* GCP의 Cloud Run을 사용하여 매일 22시에 Firestore에 영화정보를 업데이트하는 컨테이너를 실행하는 작업을 자동화한다.

