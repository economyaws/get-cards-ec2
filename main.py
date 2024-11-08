import asyncio
import aiohttp
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware  

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite requisições de qualquer origem
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos os métodos HTTP (GET, POST, etc)
    allow_headers=["*"],  # Permite todos os cabeçalhos
)

token_id = 'mz71n43h67yp3fni'  # Seu token de acesso   
host = 'economyenergy.bitrix24.com.br'  # O host do seu Bitrix24
user = 1  # ID do usuário

class EmailRequest(BaseModel):
    email: str

async def fetch(session, url, params):
    async with session.post(url, json=params) as response:
        response.raise_for_status()
        return await response.json()

async def get_leads(session, email_filter):
    url = f'https://{host}/rest/{user}/{token_id}/crm.lead.list/'
    params = {
        'order': {'DATE_CREATE': 'DESC'},
        'filter': {'UF_CRM_1717008267006': email_filter},
        'select': ['ID', 'OPPORTUNITY', 'STATUS_ID', 'UF_CRM_1717008267006', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'PHONE', 'UF_CRM_1717008267006', 'UF_CRM_1716238809742', 'UF_CRM_1721931621996'],
    }
    return await fetch(session, url, params)

async def get_deals(session, email_filter):
    url = f'https://{host}/rest/{user}/{token_id}/crm.deal.list/'
    params = {
        'order': {'DATE_CREATE': 'DESC'},
        'filter': {'UF_CRM_6657792586A0F': email_filter},
        'select': ['PHONE', 'CATEGORY_ID', 'OPPORTUNITY', 'STAGE_ID', 'UF_CRM_6657792586A0F', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'UF_CRM_1716235306165', 'UF_CRM_1709207938786', 'UF_CRM_664BBA75E0765', 'UF_CRM_1709060681601', 'UF_CRM_1716236663328', 'UF_CRM_1716235986482'],
    }
    return await fetch(session, url, params)

async def get_contact_phone(session, contact_id):
    if not contact_id:
        return None
    url = f'https://{host}/rest/{user}/{token_id}/crm.contact.get.json'
    params = {'id': contact_id}
    response = await fetch(session, url, params)
    return response.get('result', {}).get('PHONE', [])

async def get_data(session, email_filter):
    leads_task = get_leads(session, email_filter)
    deals_task = get_deals(session, email_filter)
    return await asyncio.gather(leads_task, deals_task)

@app.post("/get_data")
async def get_data_endpoint(request: EmailRequest):
    async with aiohttp.ClientSession() as session:
        email_filter = request.email
        
        # Obter leads e deals simultaneamente
        leads, deals = await get_data(session, email_filter)

        all_data = []

        # Processar leads
        if leads and 'result' in leads:
            result_leads = leads['result']
            for lead in result_leads:
                if 'PHONE' in lead and isinstance(lead['PHONE'], list) and len(lead['PHONE']) > 0:
                    lead['PHONE'] = lead['PHONE'][0]['VALUE']
                else:
                    lead['PHONE'] = None
                all_data.append(lead)
        else:
            raise HTTPException(status_code=404, detail="Leads not found")

        # Processar deals
        if deals and 'result' in deals:
            result_deals = deals['result']
            tasks = [get_contact_phone(session, deal.get('CONTACT_ID')) for deal in result_deals]
            phones = await asyncio.gather(*tasks)

            for deal, phone in zip(result_deals, phones):
                if phone and len(phone) > 0:
                    deal['PHONE'] = phone[0]['VALUE']
                else:
                    deal['PHONE'] = None
                all_data.append(deal)

            return {"data": all_data, "statusbody": 200}
        else:
            raise HTTPException(status_code=404, detail="Deals not found")

if __name__ == "_main_":
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(app())
