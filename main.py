import os
import json
import time
import asyncio
import logging
import sqlite3
import re
from contextlib import closing
from typing import List, Set

from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    WebAppInfo,
    MenuButtonWebApp,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)
print("=== LUX ARISTOKRAT MULTILANG ADMIN VERSION FIXED ===")

BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("API_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_API_TOKEN") or "").strip()
WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip() or "https://tahirovdd-lang.github.io/QR-project-Lux-Aristokrat/?v=1"
DB_PATH = os.getenv("DB_PATH", "lux_aristokrat.db").strip() or "lux_aristokrat.db"
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "0").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
logging.info("WEBAPP_URL (effective) = %s", WEBAPP_URL)
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN / API_TOKEN / TELEGRAM_BOT_TOKEN is not set")


def parse_admin_ids() -> List[int]:
    result: Set[int] = set()
    try:
        admin_id = int(ADMIN_ID_RAW)
        if admin_id > 0:
            result.add(admin_id)
    except Exception:
        pass
    if ADMIN_IDS_RAW:
        for part in ADMIN_IDS_RAW.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                value = int(part)
                if value > 0:
                    result.add(value)
            except Exception:
                continue
    return sorted(result)

ADMIN_IDS = parse_admin_ids()
logging.info("ADMIN_ID from env: %s", ADMIN_ID_RAW)
logging.info("ADMIN_IDS from env: %s", ADMIN_IDS if ADMIN_IDS else "EMPTY")


def build_bot_session() -> AiohttpSession:
    timeout = ClientTimeout(total=75, connect=20, sock_connect=20, sock_read=60)
    connector = TCPConnector(ssl=False, limit=100, ttl_dns_cache=300, enable_cleanup_closed=True)
    client_session = ClientSession(timeout=timeout, connector=connector, trust_env=True)
    return AiohttpSession(session=client_session)

session = build_bot_session()
bot = Bot(token=BOT_TOKEN, session=session, parse_mode=ParseMode.HTML)
dp = Dispatcher()

RECENT_SCANS = {}
SCAN_TTL_SECONDS = 8


def cleanup_recent_scans():
    now = time.time()
    for k in [k for k, ts in RECENT_SCANS.items() if now - ts > SCAN_TTL_SECONDS]:
        RECENT_SCANS.pop(k, None)


def is_duplicate_scan(user_id: int, code: str) -> bool:
    cleanup_recent_scans()
    key = f"{user_id}:{code}"
    now = time.time()
    if key in RECENT_SCANS:
        return True
    RECENT_SCANS[key] = now
    return False

TEXTS = {
    "ru": {
        "welcome": "Добро пожаловать в <b>Lux Aristokrat</b>\n\nОткройте mini app для сканирования QR / Data Matrix.",
        "open_scanner": "Открыть сканер",
        "menu_button": "Сканер QR",
        "empty_code": "Пустой код.",
        "bad_data": "Ошибка чтения данных.",
        "scan_duplicate": "Этот код уже был только что обработан.",
        "processing_error": "Ошибка обработки QR-кода.",
        "unknown_action": "Неизвестное действие.",
        "admin_only": "Команда только для администратора.",
        "debug_url": "Текущий WEBAPP_URL:\n{url}",
        "help": "Доступные команды:\n/start — старт\n/help — помощь\n/debug_url — показать WebApp URL (admin)\n/id — показать Telegram ID\n/addqr CODE 10 — добавить QR\n/delqr CODE — удалить QR\n/listqr — список QR\n/bonus USER_ID 50 — начислить бонусы\n/balance USER_ID — баланс пользователя",
        "your_id": "Ваш Telegram ID: <code>{user_id}</code>\nAdmin access: <b>{admin}</b>",
        "cmd_usage_addqr": "Использование: <code>/addqr CODE 10</code>",
        "cmd_usage_delqr": "Использование: <code>/delqr CODE</code>",
        "cmd_usage_bonus": "Использование: <code>/bonus USER_ID 50</code>",
        "cmd_usage_balance": "Использование: <code>/balance USER_ID</code>",
        "addqr_ok": "QR добавлен:\n<code>{code}</code>\nБонусов: <b>{points}</b>",
        "addqr_exists": "QR уже существует:\n<code>{code}</code>",
        "delqr_ok": "QR удалён:\n<code>{code}</code>",
        "delqr_not_found": "QR не найден:\n<code>{code}</code>",
        "listqr_empty": "Список QR пуст.",
        "listqr_header": "Список QR-кодов:\n\n{items}",
        "bonus_ok": "Начислено <b>{amount}</b> бонусов пользователю <code>{user_id}</code>.\nНовый баланс: <b>{balance}</b>",
        "balance_text": "Баланс пользователя <code>{user_id}</code>: <b>{balance}</b>",
        "invalid_number": "Некорректное число.",
        "internal_error": "Внутренняя ошибка.",
    },
    "uz": {}, "en": {}, "tj": {},
}
TEXTS["uz"] = TEXTS["ru"].copy(); TEXTS["en"] = TEXTS["ru"].copy(); TEXTS["tj"] = TEXTS["ru"].copy()
TEXTS["uz"].update({"welcome":"<b>Lux Aristokrat</b> ga xush kelibsiz\n\nQR / Data Matrix skanerlash uchun mini app ni oching.","open_scanner":"Skanerni ochish","menu_button":"QR skaner"})
TEXTS["en"].update({"welcome":"Welcome to <b>Lux Aristokrat</b>\n\nOpen the mini app to scan QR / Data Matrix.","open_scanner":"Open scanner","menu_button":"QR Scanner"})
TEXTS["tj"].update({"welcome":"Хуш омадед ба <b>Lux Aristokrat</b>\n\nБарои скан кардани QR / Data Matrix mini app-ро кушоед.","open_scanner":"Кушодани сканер","menu_button":"Сканери QR"})

SCAN_TEXTS = {
    "ru": {
        "scan_ok_title": "✅ Бонусы начислены",
        "scan_ok_text": "{title}\n\nКод: <code>{code}</code>\nНачислено бонусов: <b>{points}</b>\nВаш баланс: <b>{balance}</b>",
        "scan_used_title": "⚠️ Код уже использован",
        "scan_used_text": "{title}\n\nКод: <code>{code}</code>\nЭтот QR-код уже был активирован ранее.",
        "scan_invalid_title": "❌ Неверный код",
        "scan_invalid_text": "{title}\n\nКод: <code>{code}</code>\nТакой QR-код не найден или недоступен.",
    }
}
SCAN_TEXTS["uz"] = SCAN_TEXTS["ru"].copy(); SCAN_TEXTS["en"] = SCAN_TEXTS["ru"].copy(); SCAN_TEXTS["tj"] = SCAN_TEXTS["ru"].copy()


def normalize_lang(lang_code: str) -> str:
    lang_code = (lang_code or "").strip().lower()
    if lang_code.startswith("ru"): return "ru"
    if lang_code.startswith("uz"): return "uz"
    if lang_code.startswith("en"): return "en"
    if lang_code.startswith("tg") or lang_code.startswith("tj"): return "tj"
    return "ru"


def get_user_lang(message: Message) -> str:
    try:
        return normalize_lang((message.from_user.language_code if message.from_user else "ru") or "ru")
    except Exception:
        return "ru"


def t(lang: str, key: str, **kwargs) -> str:
    text = (TEXTS.get(lang) or TEXTS["ru"]).get(key) or TEXTS["ru"].get(key) or key
    return text.format(**kwargs) if kwargs else text


def scan_t(lang: str, key: str, **kwargs) -> str:
    text = (SCAN_TEXTS.get(lang) or SCAN_TEXTS["ru"]).get(key) or SCAN_TEXTS["ru"].get(key) or key
    return text.format(**kwargs) if kwargs else text


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def build_main_keyboard(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=t(lang, "open_scanner"), web_app=WebAppInfo(url=WEBAPP_URL))]], resize_keyboard=True)

# QR / DATA MATRIX: старые + новые данные. Строки формата: КОД<TAB/SPACE>БАЛЛЫ
DEFAULT_QR_CODES_RAW = r'''
047000280627027'CK:&Y 3 балла
047000280627027;XKnbi 3 балла
047000280614777Vn1fbn 3 балла
047000280614777l;BmZq 3 балла
047000280614777fGQ((R 3 балла
047000280614777%2QEre 3 балла
047000280614777WV(iR% 3 балла
047000280614777Rk-,:E 3 балла
047000280613787K5lHH& 3 балла
047000280613787YZCr&M 3 балла
047000280613787%UK+oF 3 балла
047000280613787dphL6? 3 балла
0470002806137872xwygi 3 балла
047000280613787Pja<7m 3 балла
047000280613787erRd&b 3 балла
04660105982463AepmEMo 8 баллов
04660105982463aFNFj*p 8 баллов
04660105982463aK(HpFb 8 баллов
04660105982463Ak9M.Dk 8 баллов
04660105982463ISANs>B 8 баллов
04660105982463iSGUAm- 8 баллов
04660105982463IU*qyc) 8 баллов
04660105982463IUIAB5m 8 баллов
04680575072532EEnr2qI 8 баллов
04680575072532EFoXTWq 8 баллов
046807136968047%MhcKL 10 баллов
04680713692769aa>3PDC 10 баллов
04680713692769GGoFN<h 10 баллов
046807136932167i6;hP1 10 баллов
046807136932167Ipa(.' 10 баллов
04650190050321*uokRCo 5 баллов
04650190050321*cB=jhp 5 баллов
046501900503217dGF5Wm 5 баллов
046501900503216XmntNK 5 баллов
04650190050321,,TtY3K 5 баллов
046501900503216zvTDiT 5 баллов
046501900503216T'fDeY 5 баллов
04650190050314cw;jn1z 5 баллов
04650190050314B=eMboq 5 баллов
04650190052714lH=tOP4 5 баллов
04650190052714Vd)!A8Y 5 баллов
04650190052714LdX-3n& 5 баллов
04650190052714vh<R<Py 5 баллов
04650190052714VAMBxSC 5 баллов
04650190052714vfm0OGT 5 баллов
04650190052714LESs'Qt 5 баллов
04650190052714lgf&LB+ 5 баллов
04650190052714unMWR_J 5 баллов
04650190052714UNrLlRK 5 баллов
04650190052714uBBEK+T 5 баллов
04650190052714LdMWMQc 5 баллов
04650190052714V?DGUne 5 баллов
04650190052714U-ugPno 5 баллов
04650190052714UmDGrZl 5 баллов
04650259257210)ei6aHE 5 баллов
046502592572106UUCWMH 5 баллов
04650259257210KYsmesv 5 баллов
04650259257210D97X)Lc 5 баллов
04650190058655A%dqIdh 2 балла
04650190058655bu;jfel 2 балла
04650190058655bDepcKl 2 балла
046501900586559GkngRq 2 балла
04650190058655BcoTlFs 2 балла
04650190058655jnm_*VJ 2 балла
04650190058655bu=Y-RD 2 балла
04650190058655fZ8X?&e 2 балла
04650190058655AhOvJvs 2 балла
04650190058655gwXKILQ 2 балла
04650190058655AJsGQpg 2 балла
04650190058655J0kvtHM 2 балла
04650190058655jNP8tn* 2 балла
04650190058655a!4!c>" 2 балла
04650190058655aa3STfh 2 балла
046502592591843<a"r-T 2 балла
04650259259184+pStPZk 2 балла
046502592591843YSZJrq 2 балла
0465025925918419RmlRO 2 балла
046502592591842,CLi2< 2 балла
046502592591841(>lfMn 2 балла
046502592591840gu"Buc 2 балла
04650259259184=yI2X>+ 2 балла
046502592591841A(bvuw 2 балла
046502592591843kRGPo_ 2 балла
04650190050970nP/FQJD 2 балла
04650190050970m2ULH6< 2 балла
04650190050970pTgsgTX 2 балла
04650190050970qrHQoDi 2 балла
04650190050970OpBXs1R 2 балла
04650190050970OKcf,A* 2 балла
04650190050970ZkoqAxd 2 балла
04650190050970pPFtNWU 2 балла
04650190050970zg=xTHX 2 балла
04650190050970MpOV"=O 2 балла
04650190050970ms6bHof 2 балла
04650190050970mdHo3EV 2 балла
04650190050970nJCJeQf 2 балла
04650190050970Nl(8+HJ 2 балла
04650190050970ZjI1DMo 2 балла
04650190050970n6bZ1qu 2 балла
04650190050970Zi9IXTm 2 балла
04650190050970mVIw0Tt 2 балла
04650190050970P3QyiCY 2 балла
04650190050970pAKtDYO 2 балла
04650190051083BF+LEb; 2 балла
046501900510835'jFBOQ 2 балла
04650190051083>h7;kKa 2 балла
04650190051083A<leBgo 2 балла
04650190051083<XtWuYz 2 балла
04650190051083bqRvZWq 2 балла
04650190051083>G.h;a- 2 балла
04650190051083.0I&oSD 2 балла
046501900510835VVOADM 2 балла
04650190051083>QHm3fR 2 балла
04650259255933HWGt-.h 2 балла
04650259255933iasiI%Z 2 балла
04650259255933H4d4UbT 2 балла
04650259255933i39ntER 2 балла
04650259255933hzHTfac 2 балла
04650259255933HojrAIb 2 балла
04650259255933GWXHsdZ 2 балла
04650259255933cokBrwh 2 балла
04650259255933cSqACQd 2 балла
04650259255933CN!L:.A 2 балла
04650259255933cndsrsX 2 балла
04650259255933GLqhgao 2 балла
04650259255933dytio+l 2 балла
04650259255933hs=F8(n 2 балла
04650259255933DKK>hBa 2 балла
04650259255933ChmvSQI 2 балла
04650259255933hD)2ulp 2 балла
04650259255933dtfiC)i 2 балла
04650259255933Hwwod"u 2 балла
04650259255933e=lcCDN 2 балла
04650259252680*KT/H&' 2 балла
04650259252680UYbBiMP 2 балла
04650259252680TkFCpm3 2 балла
04650259252680WzSMlBD 2 балла
04650259252680UM%vor> 2 балла
04650259252680xskcS3B 2 балла
04650259254172YlXsXTf 2 балла
04650259254172t!tdG3% 2 балла
04650259254172U.XL31* 2 балла
04650259254172SGkY*!U 2 балла
04650259254172sZnn,fY 2 балла
04650259254172u4Va3Dj 2 балла
04650259250891X6hbJ.k 2 балла
04650259250891RVGlsJi 2 балла
04650259250891AF>mcIQ 2 балла
04650259250891aXXI&XV 2 балла
04650259254226%X-!eKV 2 балла
04650259254226BAiHQLt 2 балла
04650259254226!tcCN,i 2 балла
046502592542261.NoV?k 2 балла
046502592542266LN%NGK 2 балла
04650259254226<Dp2d!3 2 балла
04650190050932?GXcHVh 2 балла
04650190050932jTbwTx? 2 балла
04650190050932&m'6VAI 2 балла
04650190050932tO:UU:H 2 балла
04650190050932ubH=XlK 2 балла
04650190050932k_c,CkV 2 балла
04650190050734F58nQqA 2 балла
04650190050734Fw?_DNf 2 балла
04650190050734Iyo*aXh 2 балла
04650190050734H1r)CSO 2 балла
046501900507348HhZSLk 2 балла
04603725791107LiblR<W 2 балла
046037257911078_29Aim 2 балла
04603725791107KEa;)Kk 2 балла
04603725791107k(hk.ow 2 балла
04650190051076lrZtjWN 2 балла
04650190051076r8HXlDb 2 балла
04650190051076U=;A(:e 2 балла
04650190051076ZMX1-rE 2 балла
04650190051076zElBY<R 2 балла
04650190051076QDMq5rj 2 балла
04650190051076QVhdpEa 2 балла
04650190051076lehQv!2 2 балла
04650190051076-Xktc!E 2 балла
04650190051076ZsNiKZH 2 балла
046201464647007<+:2fU 5 баллов
046201464647007=1LN5Q 5 баллов
046201464647007<6GyLK 5 баллов
046201464647007=%eq5v 5 баллов
046201464647007=jM;4n 5 баллов
046201464486877v*:*UR 5 баллов
046201464486877Us1e(a 5 баллов
046201464486877n6E1)M 5 баллов
046201464486877URrndK 5 баллов
046201464486877ukH0ZB 5 баллов
046201464646877PIkoan 5 баллов
046201464646877p+3sT/ 5 баллов
046201464646877Pa4QCy 5 баллов
046201464646877PIi6Hn 5 баллов
046201464646877P3Mz_U 5 баллов
046201464646947S.6Fju 5 баллов
046201464646947RQBywS 5 баллов
046201464646947S.<'s9 5 баллов
046201464646947S*kqcP 5 баллов
046201464646947rRlmT5 5 баллов
046201464480213tuG,RG 5 баллов
046201464480213l7Jle% 5 баллов
046201464480213CjvoFd 5 баллов
0462014644802131cP!r/ 5 баллов
046201464480213CaVoAs 5 баллов
046201464480213EPnVur 5 баллов
046201464480213N'RDyK 5 баллов
046201464480213TPvhuo 5 баллов
046201464480214&CyJDe 5 баллов
046201464480147j!*7QT 5 баллов
046201464480147J!(IWP 5 баллов
046201464480147J(&!!z 5 баллов
046201464480147iX:X5; 5 баллов
046201464480147IXUA(u 5 баллов
046201464480147Iw:u>C 5 баллов
046201464480147IS4-g% 5 баллов
046201464480147J(FC%c 5 баллов
046201464480147li-gSV 5 баллов
046201464480147i0kGNz 5 баллов
046201464480147la+;kI 5 баллов
046201464480147lKYa*o 5 баллов
046201464480147lK9rHq 5 баллов
046201464480147Lm2cEq 5 баллов
046201464480147lcnP,R 5 баллов
046201464480147'lg%!( 5 баллов
046201464480147Lep&0) 5 баллов
046201464480147KAJsPv 5 баллов
046201464480147'KCDj, 5 баллов
046201464480147K82wS/ 5 баллов
046201464480147KlkIE( 5 баллов
046201464480147Kh>KEp 5 баллов
046201464480147KV!z5+ 5 баллов
046201464480147J(qn=/ 5 баллов
046201464480147KHwkO, 5 баллов
046201464480147kO>FsH 5 баллов
046201464480147KpO3:U 5 баллов
046201464480147l_WX6c 5 баллов
046201464480147kpMK/* 5 баллов
046801435771247XJR9V< 5 баллов
046801435771247xU7jeS 5 баллов
046801435771247x)(uS8 5 баллов
046801435771247xeSZoW 5 баллов
046801435771247X0a=eX 5 баллов
046801435771247X<UtPT 5 баллов
046801435771247x+;+My 5 баллов
046801435771247x24A.X 5 баллов
04680143577537WrFZTd5 5 баллов
04680143577537Wzekkmq 5 баллов
04680143577537WU_rT.u 5 баллов
04680143577537wZAf);B 5 баллов
04680143577537XJkQKns 5 баллов
046201464479017rXACLr 5 баллов
046201464479017S',Llf 5 баллов
046201464479017S*P-51 5 баллов
046201464479017S)ja+N 5 баллов
046201464478887U%uM(J 5 баллов
046201464478887UNZ*9m 5 баллов
046201464478887u(-GL> 5 баллов
046201464478887u/=>W_ 5 баллов
046201464478887tD3wYm 5 баллов
046201464478887U'<q.D 5 баллов
046201464478887tLk12l 5 баллов
046201464478887u4UGP& 5 баллов
046201464478647gK/.a+ 5 баллов
046201464478647gNH6SR 5 баллов
046201464478647GP8J!x 5 баллов
046201464478647GnZJ0L 5 баллов
046201464478647gow!H! 5 баллов
046201464478647fy*IcQ 5 баллов
046201464478647FwVn_O 5 баллов
046201464478647fY4GjK 5 баллов
046201464478647G!jn<U 5 баллов
046201464478647fY(tXH 5 баллов
046201464478647FVtcib 5 баллов
046201464478647fvq705 5 баллов
046201464478647fWO;gE 5 баллов
046201464478647gq!PO( 5 баллов
046201464478647Gp7<m< 5 баллов
046201464478647GP'1r* 5 баллов
046201464478647FVsNJV 5 баллов
046201464478647gO*WOE 5 баллов
046201464478647g-Jw(y 5 баллов
046201464478717LsBV*z 5 баллов
046201464478717LVF6(X 5 баллов
046201464478717M_)Wf2 5 баллов
046201464478717LOF0pK 5 баллов
046201464478717M,H+YL 5 баллов
046201464052847y1Rqo3 5 баллов
046201464052847y33D*3 5 баллов
046201464052847'y1R,> 5 баллов
046201464052847yb&T2_ 5 баллов
046201464052847YhP(K, 5 баллов
046201464052847YJm+%/ 5 баллов
046201464052847ykT?iv 5 баллов
046201464052847yiA+ll 5 баллов
046201464052847y-JXL8 5 баллов
046201464052847Y,Dx>X 5 баллов
046201464478577!_xz:V 5 баллов
046201464478577OBU>x+ 5 баллов
046201464478577!3%_Gb 5 баллов
046201464478577!e_5d' 5 баллов
046201464478577!=P0zS 5 баллов
046201464478577!+t=37 5 баллов
046201464478577ObayuA 5 баллов
046201464478577O=yD0& 5 баллов
046201464478577n92dU& 5 баллов
04620146447840!FGh*BG 5 баллов
04620146447840!dVtSFa 5 баллов
04620146447840ZN0%klE 5 баллов
04620146447840!k)JRb; 5 баллов
04620146447840zkbtZsc 5 баллов
04620146447840ZX4hb>X 5 баллов
04620146447840ZqPYo-G 5 баллов
04620146447840ZQhQ<aj 5 баллов
04620146447840ZTRMqLC 5 баллов
04620146447840!elFn+( 5 баллов
04620146447765CkKRIVC 5 баллов
04620146447765Ck)OLe, 5 баллов
04620146447765cLMJ!Q' 5 баллов
04620146447765cHydMdB 5 баллов
04620146447765CJhSRCp 5 баллов
04620146447765Cmn/!6z 5 баллов
04620146447765CN<LJ9G 5 баллов
04620146447765cl.5ogb 5 баллов
04620146447765coKsDWm 5 баллов
04620146447765cNaQJ'r 5 баллов
046201464226877x?Lcg0 5 баллов
046201464226877x(L*Kn 5 баллов
046201464226877X(,FPI 5 баллов
046201464226877xhxAV, 5 баллов
046201464226877XhVrAx 5 баллов
046201464226877X9Q6PJ 5 баллов
046201464226877XBR.T) 5 баллов
046201464226877x&phXv 5 баллов
046201464226877WyRWqT 5 баллов
046201464226877xcxcY5 5 баллов
0462014644767371gMS-q 5 баллов
0462014644767371/b2j8 5 баллов
0462014644767371GMMPw 5 баллов
0462014644767371-c,QX 5 баллов
0462014644767370So_2i 5 баллов
0462014644767370Z<XVI 5 баллов
0462014644767371s?JNI 5 баллов
046201464476737'1.Ph* 5 баллов
046201464480522;5NJ4C 5 баллов
046201464480521w5bcd- 5 баллов
046201464480520W'VGH* 5 баллов
046201464480520U7AKbL 5 баллов
046201464480520P?J7Wg 5 баллов
046201464480521wcBz:i 5 баллов
046201464480522?GoCaZ 5 баллов
0462014644805226isuqJ 5 баллов
046201464480522XAEjUC 5 баллов
046201464480522Xe>AYe 5 баллов
046201464481687pRyz;6 5 баллов
046201464481687pKDl44 5 баллов
046201464481687Qevg1R 5 баллов
046201464481687Q7wc6h 5 баллов
046201464481687pv&w6% 5 баллов
046201464521897tnoDLn 5 баллов
046201464521897t>!ulQ 5 баллов
046201464481137rVGt*v 5 баллов
046201464481137Rt1qh. 5 баллов
046201464481137rrx/Qs 5 баллов
046201464481137s1lb;w 5 баллов
046201464481137S:%2jG 5 баллов
046201464481137rsC8OS 5 баллов
046201464481137Rn&!2> 5 баллов
046201464481137rWb.Q= 5 баллов
046201464481137RugYPu 5 баллов
04620146448069ymu,8BE 5 баллов
04620146448069ucOSBc: 5 баллов
04620146448069USo';Y5 5 баллов
04620146449295_bM;?jd 5 баллов
04620146449295_c)PTtd 5 баллов
04620146449295.hTb&vg 5 баллов
04620146449295-.MzQ.J 5 баллов
04620146449295?YeqLxI 5 баллов
04620146449295?Zt:VN) 5 баллов
04620146449295.7mnU>Y 5 баллов
04620146449295.+nSqO1 5 баллов
04620146449295_<GOOnh 5 баллов
04620146449295.VRis>l 5 баллов
046201464495787LN%dX. 5 баллов
046201464495787KHFO&o 5 баллов
046201464495787kni<Jq 5 баллов
046201464495787LsEiz4 5 баллов
046201464495787LqlQ-b 5 баллов
046201464495787K%UNF> 5 баллов
046201464495787k;A'NP 5 баллов
046201464495787K3'wSD 5 баллов
046201464495787ji;-PQ 5 баллов
046201464495787KAPXIK 5 баллов
046201464495787lCBMXm 5 баллов
046201464495787LISGzL 5 баллов
046201464495787lRksrC 5 баллов
046201464495787ll2/<) 5 баллов
046201464495787KkzpW= 5 баллов
046201464495787K*jRR9 5 баллов
046201464495787K0wP1s 5 баллов
046201464494007eW!qk9 5 баллов
046201464494007Fbumeq 5 баллов
046201464494007f21_Ku 5 баллов
046201464494007hThM1) 5 баллов
046201464494007hU0&hT 5 баллов
046201464494007HzC6W' 5 баллов
046201464494007HlXEX. 5 баллов
046201464494007f0Bu(3 5 баллов
046201464494007f<Gh&D 5 баллов
046201464494007Fs!:-Z 5 баллов
046201464494007Fx(%Jg 5 баллов
046201464494007FxXF-= 5 баллов
046201464494007HX3TQk 5 баллов
046201464494007HX+d?P 5 баллов
046201464494007Hn'ltU 5 баллов
046201464494007F-8>vv 5 баллов
046201464494007HJr5q+ 5 баллов
046201464494007HlYap3 5 баллов
046201464494007HNuA.S 5 баллов
046201464494007fgst31 5 баллов
04620146449394se8W-je 5 баллов
04620146449394RXepp6( 5 баллов
04620146449394rqKmUdU 5 баллов
04620146449394s8r9?Mz 5 баллов
04620146449394S6:e2L+ 5 баллов
046201464573137ZtC'sh 5 баллов
046201464573137ZK0Zca 5 баллов
046201464573137Zio1>/ 5 баллов
046201464573137!J!Egx 5 баллов
046201464573137!oumJ; 5 баллов
046201464496087=)+0'I 5 баллов
046201464496087=(armd 5 баллов
046201464496087<KE9Z= 5 баллов
046201464496087+t/_Up 5 баллов
046201464496087Ym4i65 5 баллов
046201464496087ylXX=D 5 баллов
046201464496087yfggUY 5 баллов
046201464496087YrpXF. 5 баллов
046201464496087Yq4-E+ 5 баллов
046201464496087YpA_Wd 5 баллов
046201464496087Yq*_DX 5 баллов
046201464496087<<nW0T 5 баллов
046201464496087YQaqpc 5 баллов
046201464496087<gZ-.T 5 баллов
046201464496087<hFVS! 5 баллов
046201464496087<i&kr4 5 баллов
046201464496087+ULy8L 5 баллов
046201464496087<saeZ> 5 баллов
046201464496087yJS7:9 5 баллов
046201464496087YW_/nW 5 баллов
046201464495167tYH?54 5 баллов
046201464495167UImrgk 5 баллов
046201464495167U-ghai 5 баллов
046201464495167tXOzsh 5 баллов
046201464495167TY,oos 5 баллов
046201464495237!T8c&o 5 баллов
046201464495237ZN)0/S 5 баллов
046201464495237!1,:?X 5 баллов
046201464495237Kr)SKO 5 баллов
046201464495237kqu&ax 5 баллов
046201464495237!Qs5CF 5 баллов
046201464495237!Jz,&; 5 баллов
046201464495237kU6;Xa 5 баллов
046201464495237!XJ+d1 5 баллов
046201464493637XKZU&5 5 баллов
046201464493637X=,ZyW 5 баллов
046201464493637x'1g=x 5 баллов
046201464493637WsZzV9 5 баллов
046201464493637XiJENB 5 баллов
046201464494797Q<H5/S 5 баллов
046201464494797PVyN0Y 5 баллов
046201464494797Qd47+l 5 баллов
046201464494797PsdQuj 5 баллов
046201464494797ql8grP 5 баллов
046201464494797q?X=ed 5 баллов
046201464494797qDPdmW 5 баллов
046201464494797pzXvbs 5 баллов
046201464494797p+r6Bs 5 баллов
046201464494797pww=bC 5 баллов
04620146449448l5RMiJe 5 баллов
04620146449448l9QsjjW 5 баллов
04620146449448LgZcjgm 5 баллов
04620146449448Lkg(Lh? 5 баллов
04620146449448leEBIx> 5 баллов
04620146449431lMuMgne 5 баллов
04620146449431LX6Uc,i 5 баллов
04620146449431LV'G0_o 5 баллов
04620146449431LLUqbUc 5 баллов
04620146449431M*Stdcn 5 баллов
'''


def parse_default_qr_codes(raw: str):
    codes = []
    seen = set()
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(.*?)\s+(\d+)\s+бал", line, flags=re.IGNORECASE)
        if not m:
            logging.warning("Skipped QR line: %r", line)
            continue
        code = m.group(1).strip()
        points = int(m.group(2))
        if code and code not in seen:
            seen.add(code)
            codes.append((code, points))
    return codes

DEFAULT_QR_CODES = parse_default_qr_codes(DEFAULT_QR_CODES_RAW)


def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_qr_bonus_tables():
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS loyalty_users (user_id INTEGER PRIMARY KEY, language TEXT DEFAULT 'ru', bonus_balance INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS qr_codes (id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT NOT NULL UNIQUE, points INTEGER NOT NULL DEFAULT 1, is_active INTEGER NOT NULL DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS qr_code_redemptions (id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT NOT NULL, user_id INTEGER NOT NULL, points INTEGER NOT NULL DEFAULT 0, redeemed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE(code))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS bonus_transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, amount INTEGER NOT NULL, type TEXT NOT NULL, code TEXT, comment TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_qr_codes_code ON qr_codes(code)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_redemptions_user_id ON qr_code_redemptions(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bonus_transactions_user_id ON bonus_transactions(user_id)")
        conn.commit()


def seed_default_qr_codes():
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO qr_codes (code, points, is_active)
            VALUES (?, ?, 1)
            ON CONFLICT(code) DO UPDATE SET points = excluded.points, is_active = 1
        """, DEFAULT_QR_CODES)
        conn.commit()
        logging.info("Default QR/Data Matrix codes synced: %s", len(DEFAULT_QR_CODES))


def ensure_loyalty_user(conn: sqlite3.Connection, user_id: int, language: str = "ru"):
    conn.execute("""INSERT INTO loyalty_users (user_id, language, bonus_balance) VALUES (?, ?, 0) ON CONFLICT(user_id) DO UPDATE SET language = excluded.language, updated_at = CURRENT_TIMESTAMP""", (user_id, language))


def get_user_balance(conn: sqlite3.Connection, user_id: int) -> int:
    row = conn.execute("SELECT bonus_balance FROM loyalty_users WHERE user_id = ?", (user_id,)).fetchone()
    return int(row["bonus_balance"]) if row else 0


def find_qr_code(conn: sqlite3.Connection, code: str):
    return conn.execute("SELECT id, code, points, is_active FROM qr_codes WHERE code = ? LIMIT 1", (code,)).fetchone()


def is_qr_used(conn: sqlite3.Connection, code: str) -> bool:
    return conn.execute("SELECT 1 FROM qr_code_redemptions WHERE code = ? LIMIT 1", (code,)).fetchone() is not None


def redeem_qr_code(conn: sqlite3.Connection, user_id: int, code: str, language: str):
    ensure_loyalty_user(conn, user_id, language)
    conn.commit()
    qr_row = find_qr_code(conn, code)
    if not qr_row or int(qr_row["is_active"]) != 1:
        return {"status": "invalid", "points": 0, "balance": get_user_balance(conn, user_id)}
    if is_qr_used(conn, code):
        return {"status": "used", "points": 0, "balance": get_user_balance(conn, user_id)}
    points = int(qr_row["points"])
    try:
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM qr_code_redemptions WHERE code = ? LIMIT 1", (code,)).fetchone():
            conn.rollback()
            return {"status": "used", "points": 0, "balance": get_user_balance(conn, user_id)}
        conn.execute("INSERT INTO qr_code_redemptions (code, user_id, points) VALUES (?, ?, ?)", (code, user_id, points))
        conn.execute("UPDATE loyalty_users SET bonus_balance = bonus_balance + ?, language = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", (points, language, user_id))
        conn.execute("INSERT INTO bonus_transactions (user_id, amount, type, code, comment) VALUES (?, ?, 'accrual', ?, 'QR bonus accrual')", (user_id, points, code))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        return {"status": "used", "points": 0, "balance": get_user_balance(conn, user_id)}
    except Exception:
        conn.rollback()
        raise
    return {"status": "ok", "points": points, "balance": get_user_balance(conn, user_id)}


def add_qr_code(code: str, points: int = 1) -> bool:
    with closing(get_db_connection()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO qr_codes (code, points, is_active) VALUES (?, ?, 1)", (code.strip(), int(points)))
        conn.commit()
        return cur.rowcount > 0


def delete_qr_code(code: str) -> bool:
    with closing(get_db_connection()) as conn:
        cur = conn.cursor(); cur.execute("DELETE FROM qr_codes WHERE code = ?", (code.strip(),)); conn.commit(); return cur.rowcount > 0


def list_qr_codes(limit: int = 100):
    with closing(get_db_connection()) as conn:
        return conn.execute("SELECT code, points, is_active, created_at FROM qr_codes ORDER BY id DESC LIMIT ?", (limit,)).fetchall()


def ensure_user_exists(user_id: int, language: str = "ru"):
    with closing(get_db_connection()) as conn:
        ensure_loyalty_user(conn, user_id, language); conn.commit()


def admin_add_bonus(user_id: int, amount: int, language: str = "ru") -> int:
    with closing(get_db_connection()) as conn:
        ensure_loyalty_user(conn, user_id, language)
        conn.execute("UPDATE loyalty_users SET bonus_balance = bonus_balance + ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", (amount, user_id))
        conn.execute("INSERT INTO bonus_transactions (user_id, amount, type, comment) VALUES (?, ?, 'manual', 'Admin manual bonus')", (user_id, amount))
        conn.commit()
        return get_user_balance(conn, user_id)


def get_balance_by_user_id(user_id: int, language: str = "ru") -> int:
    with closing(get_db_connection()) as conn:
        ensure_loyalty_user(conn, user_id, language); conn.commit(); return get_user_balance(conn, user_id)


async def process_scanned_qr_code(message: Message, code: str, language: str) -> None:
    user_id = message.from_user.id if message.from_user else 0
    try:
        with closing(get_db_connection()) as conn:
            result = redeem_qr_code(conn, user_id, code, language)
        status = result["status"]; points = int(result.get("points", 0)); balance = int(result.get("balance", 0))
        if status == "ok":
            title = scan_t(language, "scan_ok_title")
            await message.answer(scan_t(language, "scan_ok_text", title=title, code=code, points=points, balance=balance)); return
        if status == "used":
            title = scan_t(language, "scan_used_title")
            await message.answer(scan_t(language, "scan_used_text", title=title, code=code)); return
        title = scan_t(language, "scan_invalid_title")
        await message.answer(scan_t(language, "scan_invalid_text", title=title, code=code))
    except Exception as e:
        logging.exception("process_scanned_qr_code error: %s", e)
        await message.answer(t(language, "processing_error"))

@dp.message(CommandStart())
async def start_handler(message: Message):
    lang = get_user_lang(message)
    ensure_user_exists(message.from_user.id if message.from_user else 0, lang)
    await message.answer(t(lang, "welcome"), reply_markup=build_main_keyboard(lang))

@dp.message(Command("help"))
async def help_handler(message: Message):
    await message.answer(t(get_user_lang(message), "help"))

@dp.message(Command("id"))
async def id_handler(message: Message):
    lang = get_user_lang(message); user_id = message.from_user.id if message.from_user else 0
    await message.answer(t(lang, "your_id", user_id=user_id, admin="YES" if is_admin(user_id) else "NO"))

@dp.message(Command("debug_url"))
async def debug_url_handler(message: Message):
    lang = get_user_lang(message); user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id): await message.answer(t(lang, "admin_only")); return
    await message.answer(t(lang, "debug_url", url=WEBAPP_URL))

@dp.message(Command("addqr"))
async def addqr_handler(message: Message):
    lang = get_user_lang(message); user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id): await message.answer(t(lang, "admin_only")); return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3: await message.answer(t(lang, "cmd_usage_addqr")); return
    code = parts[1].strip()
    try: points = int(parts[2].strip())
    except Exception: await message.answer(t(lang, "invalid_number")); return
    if points <= 0: await message.answer(t(lang, "invalid_number")); return
    try:
        await message.answer(t(lang, "addqr_ok" if add_qr_code(code, points) else "addqr_exists", code=code, points=points))
    except Exception as e:
        logging.exception("addqr error: %s", e); await message.answer(t(lang, "internal_error"))

@dp.message(Command("delqr"))
async def delqr_handler(message: Message):
    lang = get_user_lang(message); user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id): await message.answer(t(lang, "admin_only")); return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: await message.answer(t(lang, "cmd_usage_delqr")); return
    code = parts[1].strip()
    try: await message.answer(t(lang, "delqr_ok" if delete_qr_code(code) else "delqr_not_found", code=code))
    except Exception as e: logging.exception("delqr error: %s", e); await message.answer(t(lang, "internal_error"))

@dp.message(Command("listqr"))
async def listqr_handler(message: Message):
    lang = get_user_lang(message); user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id): await message.answer(t(lang, "admin_only")); return
    try:
        rows = list_qr_codes(limit=100)
        if not rows: await message.answer(t(lang, "listqr_empty")); return
        items = [f"• <code>{row['code']}</code>\n  points: <b>{row['points']}</b> | {'active' if int(row['is_active']) == 1 else 'inactive'}" for row in rows]
        await message.answer(t(lang, "listqr_header", items="\n\n".join(items)))
    except Exception as e: logging.exception("listqr error: %s", e); await message.answer(t(lang, "internal_error"))

@dp.message(Command("bonus"))
async def bonus_handler(message: Message):
    lang = get_user_lang(message); user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id): await message.answer(t(lang, "admin_only")); return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3: await message.answer(t(lang, "cmd_usage_bonus")); return
    try: target_user_id = int(parts[1].strip()); amount = int(parts[2].strip())
    except Exception: await message.answer(t(lang, "invalid_number")); return
    if amount == 0: await message.answer(t(lang, "invalid_number")); return
    try:
        balance = admin_add_bonus(target_user_id, amount, lang)
        await message.answer(t(lang, "bonus_ok", user_id=target_user_id, amount=amount, balance=balance))
    except Exception as e: logging.exception("bonus error: %s", e); await message.answer(t(lang, "internal_error"))

@dp.message(Command("balance"))
async def balance_handler(message: Message):
    lang = get_user_lang(message); user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id): await message.answer(t(lang, "admin_only")); return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: await message.answer(t(lang, "cmd_usage_balance")); return
    try: target_user_id = int(parts[1].strip())
    except Exception: await message.answer(t(lang, "invalid_number")); return
    try:
        balance = get_balance_by_user_id(target_user_id, lang)
        await message.answer(t(lang, "balance_text", user_id=target_user_id, balance=balance))
    except Exception as e: logging.exception("balance error: %s", e); await message.answer(t(lang, "internal_error"))

@dp.message(F.web_app_data)
async def handle_web_app_data(message: Message):
    raw_data = message.web_app_data.data if message.web_app_data else ""
    logging.info("WEB_APP_DATA RAW: %s", raw_data)
    try: data = json.loads(raw_data)
    except Exception as e:
        logging.exception("WEB_APP_DATA JSON parse error: %s", e); await message.answer(t(get_user_lang(message), "bad_data")); return
    action = str(data.get("action", "")).strip()
    code = str(data.get("code", "")).strip()
    language = normalize_lang(str(data.get("language", "ru")).strip().lower() or "ru")
    if action != "scan_qr":
        await message.answer(t(language, "unknown_action")); return
    if not code:
        await message.answer(t(language, "empty_code")); return
    user_id = message.from_user.id if message.from_user else 0
    if is_duplicate_scan(user_id, code):
        logging.info("Duplicate scan ignored: user_id=%s code=%s", user_id, code); return
    await process_scanned_qr_code(message, code, language)

async def set_menu_button():
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text=TEXTS["ru"]["menu_button"], web_app=WebAppInfo(url=WEBAPP_URL)))
        logging.info("Menu button WebApp set successfully")
    except Exception as e:
        logging.exception("Failed to set menu button: %s", e)

async def on_startup():
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        logging.info("Webhook deleted successfully")
    except Exception as e:
        logging.exception("Failed to delete webhook: %s", e)
    init_qr_bonus_tables()
    seed_default_qr_codes()
    await set_menu_button()
    logging.info("Bot started successfully")

async def on_shutdown():
    try: await bot.session.close()
    except Exception: pass
    logging.info("Bot stopped")

async def main():
    await on_startup()
    try:
        await dp.start_polling(bot, polling_timeout=30, allowed_updates=dp.resolve_used_update_types(), handle_signals=True, close_bot_session=False)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): logging.info("Application interrupted")
