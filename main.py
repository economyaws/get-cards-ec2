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

# Função para obter os leads com ID > last_id e start=-1
async def get_leads(session, email_filter, last_id=None):
    url = f'https://{host}/rest/{user}/{token_id}/crm.lead.list/'
    params = {
        'order': {'ID': 'ASC'},  # Ordena os leads por ID (crescente)
        'filter': {'UF_CRM_1717008267006': email_filter},
        'select': ['ID', 'OPPORTUNITY', 'STATUS_ID', 'UF_CRM_1717008267006', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'PHONE', 'UF_CRM_1717008267006', 'UF_CRM_1716238809742', 'UF_CRM_1721931621996'],
        'start': -1,  # Desabilita o contador de elementos
        'limit': 50  # Define o limite por página
    }

    if last_id:
        params['filter']['ID'] = {'>': last_id}  # Filtra para IDs maiores que o último ID recebido

    all_leads = []
    while True:
        response = await fetch(session, url, params)
        if 'result' in response:
            all_leads.extend(response['result'])
        
        # Verificar se há mais resultados para buscar
        if len(response.get('result', [])) < params['limit']:
            break
        
        # Atualiza o last_id com o ID do último item recebido
        last_id = response['result'][-1]['ID']
        params['filter']['ID'] = {'>': last_id}  # Define a condição de filtro para IDs maiores

    return all_leads

# Função para obter os deals com ID > last_id e start=-1
async def get_deals(session, email_filter, last_id=None):
    url = f'https://{host}/rest/{user}/{token_id}/crm.deal.list/'
    params = {
        'order': {'ID': 'ASC'},  # Ordena os deals por ID (crescente)
        'filter': {'UF_CRM_6657792586A0F': email_filter},
        'select': ['PHONE', 'CATEGORY_ID', 'OPPORTUNITY', 'STAGE_ID', 'UF_CRM_6657792586A0F', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'UF_CRM_1716235306165', 'UF_CRM_1709207938786', 'UF_CRM_664BBA75E0765', 'UF_CRM_1709060681601', 'UF_CRM_1716236663328', 'UF_CRM_1716235986482'],
        'start': -1,  # Desabilita o contador de elementos
        'limit': 50  # Define o limite por página
    }

    if last_id:
        params['filter']['ID'] = {'>': last_id}  # Filtra para IDs maiores que o último ID recebido

    all_deals = []
    while True:
        response = await fetch(session, url, params)
        if 'result' in response:
            all_deals.extend(response['result'])
        
        # Verificar se há mais resultados para buscar
        if len(response.get('result', [])) < params['limit']:
            break
        
        # Atualiza o last_id com o ID do último item recebido
        last_id = response['result'][-1]['ID']
        params['filter']['ID'] = {'>': last_id}  # Define a condição de filtro para IDs maiores

    return all_deals

# Função para obter o telefone do contato
async def get_contact_phone(session, contact_id):
    if not contact_id:
        return None
    url = f'https://{host}/rest/{user}/{token_id}/crm.contact.get.json'
    params = {'id': contact_id}
    response = await fetch(session, url, params)
    return response.get('result', {}).get('PHONE', [])

# Função para processar os dados simultaneamente
async def get_data(session, email_filter):
    leads = await get_leads(session, email_filter)
    deals = await get_deals(session, email_filter)
    return leads, deals

@app.post("/get_data")
async def get_data_endpoint(request: EmailRequest):
    async with aiohttp.ClientSession() as session:
        email_filter = request.email

        # Obter leads e deals simultaneamente
        leads, deals = await get_data(session, email_filter)

        all_data = []

        # Processar leads
        if leads:
            for lead in leads:
                if 'PHONE' in lead and isinstance(lead['PHONE'], list) and len(lead['PHONE']) > 0:
                    lead['PHONE'] = lead['PHONE'][0]['VALUE']
                else:
                    lead['PHONE'] = None
                all_data.append(lead)
        else:
            raise HTTPException(status_code=404, detail="Leads not found")

        # Processar deals
        if deals:
            tasks = [get_contact_phone(session, deal.get('CONTACT_ID')) for deal in deals]
            phones = await asyncio.gather(*tasks)

            for deal, phone in zip(deals, phones):
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
