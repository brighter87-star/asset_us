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
        max_pages = 10  # Safety limit to prevent infinite loop
        page = 0

        while page < max_pages:
            page += 1
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

        if page >= max_pages:
            print(f"[WARN] Holdings pagination hit max pages ({max_pages})")

        return all_holdings

    def get_sellable_quantity(self, symbol: str) -> int:
        """
        특정 종목의 매도가능수량 조회.
        3개 거래소(NASD, NYSE, AMEX)에서 검색.

        Args:
            symbol: 종목코드

        Returns:
            int: 매도가능수량 (없으면 0)
        """
        exchanges = ["NASD", "NYSE", "AMEX"]
        for exchange in exchanges:
            try:
                holdings = self.get_holdings(exchange_code=exchange)
                for h in holdings:
                    if h.get("ovrs_pdno") == symbol:
                        # ovrs_cblc_qty: 해외체결기준수량 (실제 보유수량)
                        qty = int(h.get("ovrs_cblc_qty", 0) or 0)
                        return qty
            except Exception as e:
                print(f"[WARN] Failed to check holdings on {exchange}: {e}")
                continue
        return 0

    def get_account_balance(self, exchange_code="NASD", currency="USD"):
        """
        해외주식 계좌잔고 조회 (외화잔고 포함)

        TR_ID: TTTS3012R

        Returns:
            dict: 계좌 요약 정보
                - frcr_evlu_amt2: 외화평가금액 (외화잔고 = 현금 + 주식평가)
                - frcr_use_psbl_amt: 외화사용가능금액
                - frcr_pchs_amt1: 외화매입금액
                - ovrs_tot_pfls: 해외총손익
                - tot_evlu_pfls_amt: 총평가손익금액
        """
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-balance"
        tr_id = "TTTS3012R"

        headers = self._get_headers(tr_id)

        params = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "OVRS_EXCG_CD": exchange_code,
            "TR_CRCY_CD": currency,
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }

        self._wait_for_rate_limit()

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Balance request failed: {response.status_code} - {response.text}")

        data = response.json()

        if data.get("rt_cd") != "0":
            raise Exception(f"API error: {data.get('msg_cd')} - {data.get('msg1')}")

        # output2가 계좌 요약 정보
        output2 = data.get("output2", {})
        output3 = data.get("output3", {})

        # output2가 리스트일 수 있음
        if isinstance(output2, list) and output2:
            output2 = output2[0]

        return {
            "currency": currency,
            "exchange_code": exchange_code,
            # output2 필드들
            "frcr_evlu_amt2": float(output2.get("frcr_evlu_amt2", 0) or 0),  # 외화평가금액2
            "frcr_use_psbl_amt": float(output2.get("frcr_use_psbl_amt", 0) or 0),  # 외화사용가능금액
            "frcr_pchs_amt1": float(output2.get("frcr_pchs_amt1", 0) or 0),  # 외화매입금액1
            "ovrs_tot_pfls": float(output2.get("ovrs_tot_pfls", 0) or 0),  # 해외총손익
            "tot_evlu_pfls_amt": float(output2.get("tot_evlu_pfls_amt", 0) or 0),  # 총평가손익금액
            "tot_pftrt": float(output2.get("tot_pftrt", 0) or 0),  # 총수익률
            # output3 필드들 (있을 경우)
            "raw_output2": output2,
            "raw_output3": output3,
        }

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
        tr_cont_next = ""  # 첫 요청은 공백, 다음 요청은 "N"
        max_pages = 100
        page = 0

        while page < max_pages:
            page += 1
            headers = self._get_headers(tr_id, tr_cont=tr_cont_next)

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

            # 연속조회 확인: M이면 다음 페이지 있음, D/E/공백이면 종료
            tr_cont = response.headers.get("tr_cont", "")
            if tr_cont != "M":
                break

            # 다음 페이지 조회 설정
            tr_cont_next = "N"  # 다음 요청시 연속조회 표시
            ctx_area_fk200 = data.get("ctx_area_fk200", "")
            ctx_area_nk200 = data.get("ctx_area_nk200", "")

            if not ctx_area_fk200 and not ctx_area_nk200:
                break

        if page >= max_pages:
            print(f"[WARN] Trade history pagination hit max pages ({max_pages})")

        return all_trades

    def get_current_price(self, symbol, exchange_code="NAS"):
        """
        해외주식 현재가 조회 (GET /uapi/overseas-price/v1/quotations/price)

        TR_ID: HHDFS00000300

        Args:
            symbol: 종목코드 (AAPL, TSLA 등)
            exchange_code: 거래소코드 (NAS, NYS, AMS, HKS, SHS, SZS, TSE, HNX, HSX)
                          - NAS: 나스닥, NYS: 뉴욕, AMS: 아멕스

        Returns:
            dict: 현재가 정보 (last, open, high, low, etc.)
        """
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/price"
        tr_id = "HHDFS00000300"

        headers = self._get_headers(tr_id)

        params = {
            "AUTH": "",
            "EXCD": exchange_code,
            "SYMB": symbol,
        }

        self._wait_for_rate_limit()

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Price request failed: {response.status_code} - {response.text}")

        data = response.json()

        if data.get("rt_cd") != "0":
            raise Exception(f"API error: {data.get('msg_cd')} - {data.get('msg1')}")

        output = data.get("output", {})
        return {
            "symbol": symbol,
            "last": float(output.get("last", 0)),
            "open": float(output.get("open", 0)),
            "high": float(output.get("high", 0)),
            "low": float(output.get("low", 0)),
            "base": float(output.get("base", 0)),  # 전일종가
            "diff": float(output.get("diff", 0)),  # 전일대비
            "rate": float(output.get("rate", 0)),  # 등락률
            "volume": int(output.get("tvol", 0)),
        }

    def get_daily_prices(self, symbol, exchange_code="NAS", days=6, period="0", adjust="1"):
        """
        해외주식 기간별시세 조회 (GET /uapi/overseas-price/v1/quotations/dailyprice)

        TR_ID: HHDFS76240000

        Args:
            symbol: 종목코드
            exchange_code: 거래소코드 (NAS, NYS, AMS)
            days: 가져올 일수 (기본 6 - 오늘 포함해서 넉넉하게)
            period: 0=일, 1=주, 2=월
            adjust: 0=원주가, 1=수정주가

        Returns:
            list[dict]: 일별 시세 리스트 (최신순) [{date, open, high, low, close, volume}, ...]
        """
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/dailyprice"
        tr_id = "HHDFS76240000"

        headers = self._get_headers(tr_id)

        params = {
            "AUTH": "",
            "EXCD": exchange_code,
            "SYMB": symbol,
            "GUBN": period,
            "BYMD": "",  # 빈값이면 최근일자부터
            "MODP": adjust,
        }

        self._wait_for_rate_limit()

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Daily price request failed: {response.status_code}")

        data = response.json()

        if data.get("rt_cd") != "0":
            raise Exception(f"API error: {data.get('msg_cd')} - {data.get('msg1')}")

        results = []
        for item in (data.get("output2") or [])[:days]:
            results.append({
                "date": item.get("xymd", ""),
                "open": float(item.get("open", 0)),
                "high": float(item.get("high", 0)),
                "low": float(item.get("low", 0)),
                "close": float(item.get("clos", 0)),
                "volume": int(item.get("tvol", 0)),
            })

        return results

    def get_buying_power(self, exchange_code="NASD", symbol="AAPL"):
        """
        해외주식 매수가능금액 조회

        TR_ID: TTTS3007R

        Args:
            exchange_code: 거래소코드
            symbol: 종목코드 (필수)

        Returns:
            dict: 매수가능금액 정보
        """
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-psamount"
        tr_id = "TTTS3007R"

        headers = self._get_headers(tr_id)

        params = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "OVRS_EXCG_CD": exchange_code,
            "OVRS_ORD_UNPR": "100",
            "ITEM_CD": symbol,
        }

        self._wait_for_rate_limit()

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Buying power request failed: {response.status_code} - {response.text}")

        data = response.json()

        if data.get("rt_cd") != "0":
            raise Exception(f"API error: {data.get('msg_cd')} - {data.get('msg1')}")

        output = data.get("output", {})
        return {
            "currency": output.get("tr_crcy_cd", "USD"),
            "available_amt": float(output.get("ovrs_ord_psbl_amt", 0)),
            "exchange_rate": float(output.get("exrt", 0)),
        }

    def buy_order(self, symbol, quantity, price, exchange_code="NASD", order_type="00"):
        """
        해외주식 매수 주문

        TR_ID: TTTT1002U (미국 매수)

        Args:
            symbol: 종목코드
            quantity: 주문수량
            price: 주문가격 (지정가)
            exchange_code: 거래소코드
            order_type: 주문유형 (00: 지정가)

        Returns:
            dict: 주문 결과 (주문번호 등)
        """
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/order"
        tr_id = "TTTT1002U"

        headers = self._get_headers(tr_id)

        body = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "OVRS_EXCG_CD": exchange_code,
            "PDNO": symbol,
            "ORD_QTY": str(int(quantity)),
            "OVRS_ORD_UNPR": f"{price:.2f}",
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": order_type,
        }

        self._wait_for_rate_limit()

        response = requests.post(url, headers=headers, data=json.dumps(body))

        if response.status_code != 200:
            raise Exception(f"Buy order failed: {response.status_code} - {response.text}")

        data = response.json()

        if data.get("rt_cd") != "0":
            raise Exception(f"Order error: {data.get('msg_cd')} - {data.get('msg1')}")

        output = data.get("output", {})
        return {
            "order_no": output.get("ODNO", ""),
            "order_time": output.get("ORD_TMD", ""),
            "message": data.get("msg1", ""),
        }

    def sell_order(self, symbol, quantity, price, exchange_code="NASD", order_type="00"):
        """
        해외주식 매도 주문

        TR_ID: TTTT1006U (미국 매도)

        Args:
            symbol: 종목코드
            quantity: 주문수량
            price: 주문가격 (지정가)
            exchange_code: 거래소코드
            order_type: 주문유형 (00: 지정가)

        Returns:
            dict: 주문 결과
        """
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/order"
        tr_id = "TTTT1006U"

        headers = self._get_headers(tr_id)

        body = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "OVRS_EXCG_CD": exchange_code,
            "PDNO": symbol,
            "ORD_QTY": str(int(quantity)),
            "OVRS_ORD_UNPR": f"{price:.2f}",
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": order_type,
        }

        self._wait_for_rate_limit()

        response = requests.post(url, headers=headers, data=json.dumps(body))

        if response.status_code != 200:
            raise Exception(f"Sell order failed: {response.status_code} - {response.text}")

        data = response.json()

        if data.get("rt_cd") != "0":
            raise Exception(f"Order error: {data.get('msg_cd')} - {data.get('msg1')}")

        output = data.get("output", {})
        return {
            "order_no": output.get("ODNO", ""),
            "order_time": output.get("ORD_TMD", ""),
            "message": data.get("msg1", ""),
        }

    def get_pending_orders(self, exchange_code="NASD"):
        """
        해외주식 미체결 조회

        TR_ID: TTTS3018R

        Returns:
            list: 미체결 주문 리스트
        """
        url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-nccs"
        tr_id = "TTTS3018R"

        headers = self._get_headers(tr_id)

        params = {
            "CANO": self.cano,
            "ACNT_PRDT_CD": self.acnt_prdt_cd,
            "OVRS_EXCG_CD": exchange_code,
            "SORT_SQN": "DS",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }

        self._wait_for_rate_limit()

        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            raise Exception(f"Pending orders request failed: {response.status_code} - {response.text}")

        data = response.json()

        if data.get("rt_cd") != "0":
            raise Exception(f"API error: {data.get('msg_cd')} - {data.get('msg1')}")

        return data.get("output", [])


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
