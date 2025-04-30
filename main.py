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
token_id = 'mz71n43h67yp3fni'
host = 'economyenergy.bitrix24.com.br'
user = 1

semaphore = Semaphore(50)
timeout = aiohttp.ClientTimeout(total=10)

class EmailRequest(BaseModel):
    email: str

async def fetch_with_retry(session, url, params, retries=3):
    for attempt in range(retries):
        try:
            logger.info(f"Attempting request with params: {params}")
            async with session.post(url, json=params, timeout=timeout) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info(f"Response received with status {response.status}")
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < retries - 1:
                delay = 2 ** attempt + random.uniform(0, 1)
                await asyncio.sleep(delay)
            else:
                logger.error(f"Error fetching data after {retries} retries: {e}")
                raise e

async def fetch_all_items_optimized(session, url, params, item_type=''):
    all_items = []
    last_id = 0
    batch_size = 50
    total_retrieved = 0

    logger.info(f"Starting fetch for {item_type} with filter: {params.get('filter', {})}")

    base_params = params.copy()
    base_params['order'] = {'ID': 'ASC'}
    base_params['start'] = -1

    while True:
        current_params = base_params.copy()
        current_params['filter'] = current_params.get('filter', {}).copy()
        current_params['filter']['>ID'] = last_id

        async with semaphore:
            try:
                data = await fetch_with_retry(session, url, current_params)
                
                if 'result' not in data:
                    logger.error(f"Unexpected response format for {item_type}: {data}")
                    break

                batch_items = data['result']
                
                if not batch_items:
                    logger.info(f"No more {item_type} items to retrieve. Total: {total_retrieved}")
                    break

                all_items.extend(batch_items)
                total_retrieved += len(batch_items)
                
                logger.info(f"Retrieved batch of {len(batch_items)} {item_type} items. Total: {total_retrieved}")

                if batch_items:
                    last_id = max(item['ID'] for item in batch_items)
                    logger.info(f"Last ID for {item_type}: {last_id}")

                if len(batch_items) < batch_size:
                    logger.info(f"Finished retrieving {item_type} items. Total: {total_retrieved}")
                    break

            except Exception as e:
                logger.error(f"Error fetching {item_type} items with filter {current_params['filter']}: {str(e)}")
                break

    logger.info(f"Final count for {item_type}: {len(all_items)} items")
    return all_items

async def bulk_get_contact_phones(session, contact_ids):
    if not contact_ids:
        return {}
    
    logger.info(f"Fetching phones for {len(contact_ids)} contacts")
    
    url = f'https://{host}/rest/{user}/{token_id}/crm.contact.list'
    
    params = {
        'filter': {'ID': contact_ids},
        'select': ['ID', 'PHONE'],
        'start': -1
    }
    
    try:
        response_data = await fetch_all_items_optimized(session, url, params, item_type='contacts')
        
        contact_phones = {
            str(contact['ID']): 
            (contact['PHONE'][0]['VALUE'] if contact.get('PHONE') and len(contact['PHONE']) > 0 else None)
            for contact in response_data
        }
        
        logger.info(f"Successfully fetched {len(contact_phones)} contact phones")
        return contact_phones
    
    except Exception as e:
        logger.error(f"Error fetching contact phones in bulk: {str(e)}")
        return {}
    
def deduplicate_items(items, id_field='ID'):
    """
    Remove duplicate items based on ID field
    """
    unique_items = {}
    for item in items:
        item_id = item.get(id_field)
        if item_id and item_id not in unique_items:
            unique_items[item_id] = item
    
    return list(unique_items.values())

@app.post("/get_data")
async def get_data_endpoint(request: EmailRequest):
    logger.info(f"Processing request for email: {request.email}")
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        email_filter = request.email
        
        try:
            leads_url = f'https://{host}/rest/{user}/{token_id}/crm.lead.list/'
            deals_url = f'https://{host}/rest/{user}/{token_id}/crm.deal.list/'
            deals_usina_url = f'https://{host}/rest/{user}/{token_id}/crm.deal.list/'
            
            leads_params_1 = {
                'filter': {'UF_CRM_1717008267006': email_filter},
                'select': ['ID', 'OPPORTUNITY', 'STATUS_ID', 'UF_CRM_1717008267006', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'PHONE', 'UF_CRM_1717008267006', 'UF_CRM_1716238809742', 'UF_CRM_1721931621996', 'UF_CRM_672386F259715', 'UF_CRM_LEAD_1733941150539', 'UF_CRM_1732196006400']
            }

            leads_params_2 = {
                'filter': {'UF_CRM_1738175385539': email_filter},
                'select': ['ID', 'OPPORTUNITY', 'STATUS_ID', 'UF_CRM_1717008267006', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'PHONE', 'UF_CRM_1717008267006', 'UF_CRM_1716238809742', 'UF_CRM_1721931621996', 'UF_CRM_672386F259715', 'UF_CRM_LEAD_1733941150539', 'UF_CRM_1738175385539', 'UF_CRM_1732196006400']
            }
            
            deals_params_1 = {
                'filter': {'UF_CRM_6657792586A0F': email_filter},
                'select': ['PHONE', 'CATEGORY_ID', 'OPPORTUNITY', 'STAGE_ID', 'UF_CRM_6657792586A0F', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'UF_CRM_1716235306165', 'UF_CRM_1709207938786', 'UF_CRM_664BBA75E0765', 'UF_CRM_1709060681601', 'UF_CRM_1716236663328', 'UF_CRM_1716235986482', 'UF_CRM_DEAL_1730381385146', 'UF_CRM_675AC87FDFF80', 'UF_CRM_673F36E0D9944', 'UF_CRM_1715713642']
            }
            
            deals_params_2 = {
                'filter': {'UF_CRM_1738185682048': email_filter},
                'select': ['PHONE', 'CATEGORY_ID', 'OPPORTUNITY', 'STAGE_ID', 'UF_CRM_6657792586A0F', 'TITLE', 'DATE_CREATE', 'CONTACT_ID', 'UF_CRM_1716235306165', 'UF_CRM_1709207938786', 'UF_CRM_664BBA75E0765', 'UF_CRM_1709060681601', 'UF_CRM_1716236663328', 'UF_CRM_1716235986482', 'UF_CRM_DEAL_1730381385146', 'UF_CRM_675AC87FDFF80', 'UF_CRM_1738185682048', 'UF_CRM_673F36E0D9944', 'UF_CRM_1715713642']
            }
            
            deals_usina_params = {
                'filter': {'UF_CRM_1726088862520': email_filter},
                'select': ['TITLE', 'DATE_CREATE', 'CONTACT_ID', 'OPPORTUNITY', 'STAGE_ID', 'CATEGORY_ID', 'UF_CRM_1726088862520', 'UF_CRM_1726089384094', 'UF_CRM_1726089371430', 'UF_CRM_1726089284839', 'UF_CRM_1726089103186']
            }

            # Execute all requests concurrently
            results = await asyncio.gather(
                fetch_all_items_optimized(session, leads_url, leads_params_1, 'leads_1'),
                fetch_all_items_optimized(session, leads_url, leads_params_2, 'leads_2'),
                fetch_all_items_optimized(session, deals_url, deals_params_1, 'deals_1'),
                fetch_all_items_optimized(session, deals_url, deals_params_2, 'deals_2'),
                fetch_all_items_optimized(session, deals_usina_url, deals_usina_params, 'deals_usina'),
                return_exceptions=True
            )

            # Check for exceptions in results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error in request {i}: {str(result)}")

            leads_1, leads_2, deals_1, deals_2, deals_usina = [
                [] if isinstance(r, Exception) else r for r in results
            ]

            # Combine and deduplicate leads
            all_leads = leads_1 + leads_2
            deduplicated_leads = deduplicate_items(all_leads, 'ID')
            logger.info(f"Leads before deduplication: {len(all_leads)}, after: {len(deduplicated_leads)}")
            
            # Combine and deduplicate deals
            all_deals = deals_1 + deals_2
            deduplicated_deals = deduplicate_items(all_deals, 'ID')
            logger.info(f"Deals before deduplication: {len(all_deals)}, after: {len(deduplicated_deals)}")
            
            # Deduplicate usina deals
            deduplicated_deals_usina = deduplicate_items(deals_usina, 'ID')
            
            # Collect unique contact IDs
            contact_ids = list(set(
                [str(lead.get('CONTACT_ID', '')) for lead in deduplicated_leads if lead.get('CONTACT_ID')] +
                [str(deal.get('CONTACT_ID', '')) for deal in deduplicated_deals if deal.get('CONTACT_ID')] +
                [str(item.get('CONTACT_ID', '')) for item in deduplicated_deals_usina if item.get('CONTACT_ID')]
            ))
            
            contact_phones = await bulk_get_contact_phones(session, contact_ids)
            
            # Process all items and add phone numbers
            for item in deduplicated_leads:
                contact_id = str(item.get('CONTACT_ID', ''))
                item['PHONE'] = contact_phones.get(contact_id)
                item['DATA_TYPE'] = 'LEAD'
            
            for item in deduplicated_deals:
                contact_id = str(item.get('CONTACT_ID', ''))
                item['PHONE'] = contact_phones.get(contact_id)
                item['DATA_TYPE'] = 'DEAL'
            
            for item in deduplicated_deals_usina:
                contact_id = str(item.get('CONTACT_ID', ''))
                item['PHONE'] = contact_phones.get(contact_id)
                item['DATA_TYPE'] = 'DEALS_USINA'
            
            all_data = deduplicated_leads + deduplicated_deals + deduplicated_deals_usina
            
            response_data = {
                "data": all_data,
                "total_count": len(all_data),
                "leads_count": len(deduplicated_leads),
                "deals_count": len(deduplicated_deals),
                "deals_usina_count": len(deduplicated_deals_usina),
                "statusbody": 200
            }
            
            return response_data
        
        except Exception as e:
            logger.error(f"Error processing data for email {email_filter}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

if __name__ == "__main__":
    import nest_asyncio
    nest_asyncio.apply()
