import asyncio
import aiohttp
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from asyncio import Semaphore
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
token_id = 'i7qg01i85dxi5gjx'
host = 'economyenergy.bitrix24.com.br'
user = 1487

semaphore = Semaphore(10)
timeout = aiohttp.ClientTimeout(total=15)

class EmailRequest(BaseModel):
    email: str

async def fetch_with_retry(session, url, json_body, retries=3):
    for attempt in range(retries):
        try:
            async with session.post(url, json=json_body, timeout=timeout) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < retries - 1:
                delay = 2 ** attempt + random.uniform(0, 1)
                logger.warning(f"Tentativa {attempt+1} falhou: {str(e)} - Repetindo em {delay:.2f}s")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Erro final após {retries} tentativas: {str(e)}")
                raise e

async def fetch_ids(session, list_url, filter_params, select_fields):
    all_ids = []
    start = 0

    while True:
        params = {
            **filter_params,
            "start": start,
            "select": select_fields
        }
        async with semaphore:
            data = await fetch_with_retry(session, list_url, params)
        items = data.get("result", [])
        all_ids.extend([str(item["ID"]) for item in items])
        if "next" in data:
            start = data["next"]
        else:
            break
    return all_ids

async def fetch_batch_items(session, ids, entity):
    batch_url = f"https://{host}/rest/{user}/{token_id}/batch"
    endpoint = f"crm.{entity}.get"
    full_items = []

    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        cmd = {
            f"cmd[{j}]": f"{endpoint}?ID={item_id}"
            for j, item_id in enumerate(chunk)
        }
        json_body = {"halt": False, "cmd": cmd}
        async with semaphore:
            data = await fetch_with_retry(session, batch_url, json_body)
            items = data.get("result", {}).get("result", {})
            full_items.extend(items.values())
    return full_items

def filter_fields(item, allowed_fields):
    return {k: v for k, v in item.items() if k in allowed_fields}

async def fetch_items_by_email(session, entity, email_fields, email, select_fields):
    logger.info(f"Buscando {entity}s com email {email} nos campos {email_fields}")
    all_ids = []
    list_url = f"https://{host}/rest/{user}/{token_id}/crm.{entity}.list"

    for field in email_fields:
        filter_params = {
            "filter": {field: email}
        }
        ids = await fetch_ids(session, list_url, filter_params, select_fields)
        all_ids.extend(ids)

    unique_ids = list(set(all_ids))
    logger.info(f"Total de {entity}s únicos encontrados: {len(unique_ids)}")
    items = await fetch_batch_items(session, unique_ids, entity)

    # Filtrar os campos manualmente
    allowed = set(select_fields + ["ID", "CONTACT_ID"])
    filtered_items = [filter_fields(item, allowed) for item in items]

    logger.info(f"{len(filtered_items)} {entity}s filtrados com campos selecionados")
    return filtered_items

async def bulk_get_contact_phones(session, contact_ids):
    if not contact_ids:
        return {}

    batch_url = f"https://{host}/rest/{user}/{token_id}/batch"
    contact_phones = {}

    for i in range(0, len(contact_ids), 50):
        chunk = contact_ids[i:i+50]
        cmd = {
            f"cmd[{j}]": f"crm.contact.get?ID={cid}"
            for j, cid in enumerate(chunk)
        }
        json_body = {"halt": False, "cmd": cmd}
        async with semaphore:
            data = await fetch_with_retry(session, batch_url, json_body)
            results = data.get("result", {}).get("result", {})
            for item in results.values():
                cid = str(item.get("ID"))
                phone = item.get("PHONE", [{}])[0].get("VALUE") if item.get("PHONE") else None
                contact_phones[cid] = phone
    logger.info(f"Telefones recuperados para {len(contact_phones)} contatos")
    return contact_phones

def deduplicate_items(items, id_field='ID'):
    seen = {}
    for item in items:
        key = item.get(id_field)
        if key and key not in seen:
            seen[key] = item
    return list(seen.values())

@app.post("/get_data")
async def get_data_endpoint(request: EmailRequest):
    email = request.email
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            # Campos desejados
            lead_select_fields = [
                "ID", "OPPORTUNITY", "STATUS_ID", "UF_CRM_1717008267006", "TITLE", "DATE_CREATE",
                "CONTACT_ID", "PHONE", "UF_CRM_1716238809742", "UF_CRM_1721931621996",
                "UF_CRM_672386F259715", "UF_CRM_LEAD_1733941150539", "UF_CRM_1732196006400",
                "UF_CRM_1750887023084", "UF_CRM_1738175385539"
            ]

            deal_select_fields = [
                "ID", "PHONE", "CATEGORY_ID", "OPPORTUNITY", "STAGE_ID", "UF_CRM_6657792586A0F",
                "TITLE", "DATE_CREATE", "CONTACT_ID", "UF_CRM_1716235306165", "UF_CRM_1709207938786",
                "UF_CRM_664BBA75E0765", "UF_CRM_1709060681601", "UF_CRM_1716236663328",
                "UF_CRM_1716235986482", "UF_CRM_DEAL_1730381385146", "UF_CRM_675AC87FDFF80",
                "UF_CRM_673F36E0D9944", "UF_CRM_1715713642", "UF_CRM_1750887064804",
                "UF_CRM_1738185682048"
            ]

            deals_usina_select_fields = [
                "TITLE", "DATE_CREATE", "CONTACT_ID", "OPPORTUNITY", "STAGE_ID", "CATEGORY_ID",
                "UF_CRM_1726088862520", "UF_CRM_1726089384094", "UF_CRM_1726089371430",
                "UF_CRM_1726089284839", "UF_CRM_1726089103186", "UF_CRM_1750887064804"
            ]

            # Buscar dados
            leads = await fetch_items_by_email(session, "lead", [
                "UF_CRM_1717008267006", "UF_CRM_1738175385539"
            ], email, lead_select_fields)
            for l in leads:
                l["DATA_TYPE"] = "LEAD"

            deals = await fetch_items_by_email(session, "deal", [
                "UF_CRM_6657792586A0F", "UF_CRM_1738185682048"
            ], email, deal_select_fields)
            for d in deals:
                d["DATA_TYPE"] = "DEAL"

            deals_usina = await fetch_items_by_email(session, "deal", [
                "UF_CRM_1726088862520"
            ], email, deals_usina_select_fields)
            for du in deals_usina:
                du["DATA_TYPE"] = "DEALS_USINA"

            # Deduplicar
            all_items = deduplicate_items(leads + deals + deals_usina)
            logger.info(f"Deduplicados: {len(all_items)} itens no total")

            contact_ids = list(set(
                str(item["CONTACT_ID"]) for item in all_items if item.get("CONTACT_ID")
            ))
            logger.info(f"Total de CONTACT_IDs únicos: {len(contact_ids)}")

            contact_phones = await bulk_get_contact_phones(session, contact_ids)

            for item in all_items:
                cid = str(item.get("CONTACT_ID"))
                item["PHONE"] = contact_phones.get(cid)

            logger.info("Telefones associados com sucesso")

            return {
                "data": all_items,
                "total_count": len(all_items),
                "leads_count": len(leads),
                "deals_count": len(deals),
                "deals_usina_count": len(deals_usina),
                "statusbody": 200
            }
        except Exception as e:
            logger.error(f"Erro geral: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
