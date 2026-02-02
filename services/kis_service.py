"""
Korea Investment & Securities API Client for overseas stocks.
"""

import json
import time
import requests
from datetime import datetime
from pathlib import Path
from config.settings import Settings

# 토큰 캐시 파일 경로
TOKEN_CACHE_FILE = Path(__file__).resolve().parent.parent / ".token_cache.json"


class KISAPIClient:
    """
    API client for Korea Investment & Securities overseas stock trading.
    """

    def __init__(self):
        self.settings = Settings()
        self.base_url = self.settings.BASE_URL
        self.app_key = self.settings.APP_KEY
        self.app_secret = self.settings.SECRET_KEY
        self.cano = self.settings.CANO
        self.acnt_prdt_cd = self.settings.ACNT_PRDT_CD

        self._access_token = None
        self._token_expired = None
        self._last_call_time = 0
        self._min_interval = 0.5  # 0.5초 간격

        # 파일에서 캐시된 토큰 로드
        self._load_token_cache()

    def _load_token_cache(self):
        """파일에서 캐시된 토큰 로드"""
        try:
            if TOKEN_CACHE_FILE.exists():
                with open(TOKEN_CACHE_FILE, "r") as f:
                    cache = json.load(f)
                    self._access_token = cache.get("access_token")
                    expired_str = cache.get("token_expired")
                    if expired_str:
                        self._token_expired = datetime.strptime(expired_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass  # 캐시 로드 실패 시 무시

    def _save_token_cache(self):
        """토큰을 파일에 캐시"""
        try:
            cache = {
                "access_token": self._access_token,
                "token_expired": self._token_expired.strftime("%Y-%m-%d %H:%M:%S") if self._token_expired else None,
            }
            with open(TOKEN_CACHE_FILE, "w") as f:
                json.dump(cache, f)
        except Exception:
            pass  # 캐시 저장 실패 시 무시

    def _wait_for_rate_limit(self):
        """API 호출 간 최소 간격 유지"""
        elapsed = time.time() - self._last_call_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call_time = time.time()

    def get_access_token(self):
        """
        접근토큰 발급 (POST /oauth2/tokenP)

        토큰 유효기간: 24시간
        6시간 내 재호출시 동일 토큰 반환
        1분 내 재호출시 에러 발생 -> 캐시된 토큰 사용
        """
        # 캐시된 토큰이 있고 아직 유효하면 재사용
        if self._access_token and self._token_expired:
            if datetime.now() < self._token_expired:
                return self._access_token

        url = f"{self.base_url}/oauth2/tokenP"

        headers = {
            "content-type": "application/json; charset=utf-8",
        }

        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

        self._wait_for_rate_limit()

        response = requests.post(url, headers=headers, data=json.dumps(body))

        # 토큰 발급 제한 에러 (1분당 1회) - 기존 캐시된 토큰 사용
        if response.status_code == 403:
            if self._access_token:
                return self._access_token
            raise Exception(f"Token request failed: {response.status_code} - {response.text}")

        if response.status_code != 200:
            raise Exception(f"Token request failed: {response.status_code} - {response.text}")

        data = response.json()

        if "access_token" not in data:
            raise Exception(f"No access_token in response: {data}")

        self._access_token = data["access_token"]

        # 토큰 만료 시간 파싱
        if "access_token_token_expired" in data:
            try:
                self._token_expired = datetime.strptime(
                    data["access_token_token_expired"], "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                self._token_expired = None

        # 토큰 캐시 저장
        self._save_token_cache()

        return self._access_token

    def _get_headers(self, tr_id, tr_cont=""):
        """
        API 요청용 공통 헤더 생성
        """
        token = self.get_access_token()

        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }

        if tr_cont:
            headers["tr_cont"] = tr_cont

        return headers

    def get_holdings(self, exchange_code="NASD", currency="USD"):
        """
        해외주식 잔고 조회 (GET /uapi/overseas-stock/v1/trading/inquire-balance)

        TR_ID: TTTS3012R (실전) / VTTS3012R (모의)

        Args:
            exchange_code: 거래소코드 (NASD, NYSE, AMEX, SEHK, SHAA, SZAA, TKSE, HASE, VNSE)
            currency: 통화코드 (USD, HKD, CNY, JPY, VND)

        Returns:
            list: 보유종목 리스트
        """
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-balance"
        tr_id = "TTTS3012R"

        all_holdings = []
        ctx_area_fk200 = ""
        ctx_area_nk200 = ""

        while True:
            headers = self._get_headers(tr_id)

            params = {
                "CANO": self.cano,
                "ACNT_PRDT_CD": self.acnt_prdt_cd,
                "OVRS_EXCG_CD": exchange_code,
                "TR_CRCY_CD": currency,
                "CTX_AREA_FK200": ctx_area_fk200,
                "CTX_AREA_NK200": ctx_area_nk200,
            }

            self._wait_for_rate_limit()

            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                raise Exception(f"Holdings request failed: {response.status_code} - {response.text}")

            data = response.json()

            if data.get("rt_cd") != "0":
                raise Exception(f"API error: {data.get('msg_cd')} - {data.get('msg1')}")

            # output1이 보유종목 리스트
            holdings = data.get("output1", [])
            all_holdings.extend(holdings)

            # 연속조회 확인
            tr_cont = response.headers.get("tr_cont", "")
            if tr_cont in ["D", "E", ""]:
                break

            # 다음 페이지 조회
            ctx_area_fk200 = data.get("ctx_area_fk200", "")
            ctx_area_nk200 = data.get("ctx_area_nk200", "")

            if not ctx_area_fk200 and not ctx_area_nk200:
                break

        return all_holdings

    def get_trade_history(self, start_date, end_date, exchange_code="%", sll_buy_dvsn="00"):
        """
        해외주식 주문체결내역 조회 (GET /uapi/overseas-stock/v1/trading/inquire-ccnl)

        TR_ID: TTTS3035R (실전) / VTTS3035R (모의)

        Args:
            start_date: 조회 시작일 (YYYYMMDD)
            end_date: 조회 종료일 (YYYYMMDD)
            exchange_code: 거래소코드 (% = 전체)
            sll_buy_dvsn: 매도매수구분 (00=전체, 01=매도, 02=매수)

        Returns:
            list: 체결내역 리스트
        """
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-ccnl"
        tr_id = "TTTS3035R"

        all_trades = []
        ctx_area_fk200 = ""
        ctx_area_nk200 = ""

        while True:
            headers = self._get_headers(tr_id)

            params = {
                "CANO": self.cano,
                "ACNT_PRDT_CD": self.acnt_prdt_cd,
                "PDNO": "%",  # 전종목
                "ORD_STRT_DT": start_date,
                "ORD_END_DT": end_date,
                "SLL_BUY_DVSN": sll_buy_dvsn,
                "CCLD_NCCS_DVSN": "01",  # 체결만
                "OVRS_EXCG_CD": exchange_code,
                "SORT_SQN": "DS",  # 정순
                "ORD_DT": "",
                "ORD_GNO_BRNO": "",
                "ODNO": "",
                "CTX_AREA_NK200": ctx_area_nk200,
                "CTX_AREA_FK200": ctx_area_fk200,
            }

            self._wait_for_rate_limit()

            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                raise Exception(f"Trade history request failed: {response.status_code} - {response.text}")

            data = response.json()

            if data.get("rt_cd") != "0":
                raise Exception(f"API error: {data.get('msg_cd')} - {data.get('msg1')}")

            trades = data.get("output", [])
            all_trades.extend(trades)

            # 연속조회 확인
            tr_cont = response.headers.get("tr_cont", "")
            if tr_cont in ["D", "E", ""]:
                break

            # 다음 페이지 조회
            ctx_area_fk200 = data.get("ctx_area_fk200", "")
            ctx_area_nk200 = data.get("ctx_area_nk200", "")

            if not ctx_area_fk200 and not ctx_area_nk200:
                break

        return all_trades


# 테스트용 코드
if __name__ == "__main__":
    client = KISAPIClient()

    print("=== 토큰 발급 테스트 ===")
    try:
        token = client.get_access_token()
        print(f"토큰 발급 성공: {token[:50]}...")
        print(f"토큰 만료: {client._token_expired}")
    except Exception as e:
        print(f"토큰 발급 실패: {e}")
