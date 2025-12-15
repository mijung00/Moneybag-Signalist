import random

class BotTraderSelector:
    def __init__(self):
        # 5명의 봇 페르소나 정의
        self.personas = {
            "Hunter": {
                "name": "Hunter (하이에나)",
                "style": "Reversal",
                "desc": "폭락장의 사냥꾼. 남들이 공포에 질려 던질 때 줍는 역추세 전문가.",
                "keywords": ["공포", "과매도", "반등", "줍줍"]
            },
            "Surfer": {
                "name": "Surfer (서퍼)",
                "style": "Trend",
                "desc": "상승장의 파도타기 선수. 추세가 확인되면 불타기를 주저하지 않는 공격형.",
                "keywords": ["추세", "돌파", "불타기", "가즈아"]
            },
            "Sniper": {
                "name": "Sniper (스나이퍼)",
                "style": "Momentum",
                "desc": "변동성 저격수. 조용하다가 방향이 터지는 순간 방아쇠를 당기는 승부사.",
                "keywords": ["변동성", "돌파", "급등", "타이밍"]
            },
            "Farmer": {
                "name": "Farmer (농부)",
                "style": "Season",
                "desc": "인내심 강한 농부. 지루한 횡보장에서 씨를 뿌리고 수확을 기다리는 가치투자자.",
                "keywords": ["매집", "횡보", "인내", "알트"]
            },
            "Guardian": {
                "name": "Guardian (경비병)",
                "style": "Neutral",
                "desc": "자산 지킴이. 위험할 땐 무조건 현금을 챙기는 보수적인 방어 전문가.",
                "keywords": ["방어", "현금", "관망", "리스크관리"]
            }
        }

    def select_best_strategy(self, strategies, regime_info):
        """
        현재 시장 날씨(Tactical Regime)와 전략 점수를 종합하여
        오늘의 지휘관(Commander)과 최적의 전략을 선발합니다.
        """
        tactical = regime_info.get('tactical_state', 'Neutral')
        main_regime = regime_info.get('main_regime', 'Unknown')
        
        # 1. 날씨에 따른 '추천 지휘관' 배정 (기본값)
        if tactical == "Panic_Dump":
            primary_bot = "Hunter"      # 폭락 -> 하이에나
        elif tactical in ["Strong_Uptrend", "FOMO_Pump"]:
            primary_bot = "Surfer"      # 폭등 -> 서퍼
        elif tactical == "High_Vol_Chop":
            primary_bot = "Sniper"      # 흔들기 -> 스나이퍼
        elif tactical in ["Boring_Sideways", "Grinding"]:
            primary_bot = "Farmer"      # 횡보 -> 농부
        else:
            primary_bot = "Guardian"    # 애매함 -> 경비병

        # 2. [다양성 로직] 하지만 전략 점수가 압도적이라면 지휘관 교체 가능
        # 예: 하락장(Guardian 추천)이어도, 갑자기 '변동성 돌파(Sniper)' 점수가 90점이면 스나이퍼 출동
        
        best_strategy = None
        current_highest_score = 0
        final_commander = primary_bot

        # 전체 전략 중 최고 점수 찾기
        for strat in strategies:
            if strat['score'] > current_highest_score:
                current_highest_score = strat['score']
                best_strategy = strat

        # 3. 최고 점수 전략의 주인(Type) 확인
        if best_strategy:
            winning_style = best_strategy['type']
            
            # 스타일 매칭: 전략 타입과 일치하는 봇 찾기
            for bot_key, bot_info in self.personas.items():
                if bot_info['style'] == winning_style:
                    # 기본 추천 봇과 다르더라도, 점수가 높으면 얘를 지휘관으로 임명 (쿠데타!)
                    if bot_key != primary_bot and current_highest_score >= 80:
                         final_commander = bot_key
                    break
        
        # 4. 최종 결과 포장
        commander_info = self.personas[final_commander]
        
        # 만약 선택된 봇에게 맞는 전략이 하나도 없으면 (예외처리)
        if not best_strategy:
             best_strategy = strategies[0] if strategies else {
                 "name": "No Signal", "type": "Neutral", "desc": "데이터 부족", "score": 0
             }

        # 전술 상황 코멘트 생성
        regime_comment = self._generate_regime_comment(tactical, final_commander)

        return {
            "selected_strategy": best_strategy,
            "commander": commander_info['name'],
            "commander_desc": commander_info['desc'],
            "regime_comment": regime_comment
        }

    def _generate_regime_comment(self, tactical, commander_key):
        """상황극 코멘트 생성"""
        comments = {
            "Panic_Dump": "시장이 공포에 질려 비명을 지르고 있습니다.",
            "FOMO_Pump": "광기 어린 매수세가 시장을 뒤덮었습니다.",
            "High_Vol_Chop": "위아래로 거칠게 흔드는 롤러코스터 장세입니다.",
            "Boring_Sideways": "거래량이 마르고 모두가 지루해하는 횡보장입니다.",
            "Strong_Uptrend": "거침없는 상승 추세가 이어지고 있습니다.",
            "Strong_Downtrend": "매도세가 매수세를 압도하는 하락 우위입니다.",
        }
        
        base_comment = comments.get(tactical, "시장의 방향성이 모호한 구간입니다.")
        
        # 봇의 한마디 추가
        if commander_key == "Hunter":
            return f"{base_comment} (하이에나가 피 냄새를 맡고 접근합니다.)"
        elif commander_key == "Surfer":
            return f"{base_comment} (서퍼가 파도를 탈 준비를 마쳤습니다.)"
        elif commander_key == "Sniper":
            return f"{base_comment} (스나이퍼가 조용히 타겟을 조준합니다.)"
        elif commander_key == "Farmer":
            return f"{base_comment} (농부가 묵묵히 밭을 갈고 있습니다.)"
        else:
            return f"{base_comment} (경비병이 성문을 닫고 경계를 강화합니다.)"