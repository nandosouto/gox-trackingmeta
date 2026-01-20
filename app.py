import os
import hashlib
import time
import requests
from flask import Flask, request, jsonify
import logging
import uuid
import json

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DO META ADS (FACEBOOK) ---
# Valores para GOXGAIN
DEFAULT_PIXEL_ID = '1243109087705550'
DEFAULT_ACCESS_TOKEN = 'EAAKe1WkmnOsBQggUezSYTaXiVcfAPbjdqZASeXCFU0ZCetJHfV0SqocOgECbZBJrKtSR1yaY7B1uZBLhYcddrrCSnMA7dwe5QrQlAIzqxmcFLzoT5ln5wiAvHlD6tPLrsupCxCfG57YODU5OKqIV8pZCjiaxAEIZBZAWljoQtlDnFOZBXxUGoiQ3rRJY5JzGSQZDZD'

PIXEL_ID = os.environ.get('META_PIXEL_ID', DEFAULT_PIXEL_ID)
ACCESS_TOKEN = os.environ.get('META_ACCESS_TOKEN', DEFAULT_ACCESS_TOKEN)
META_API_VERSION = 'v19.0'
META_API_URL = f'https://graph.facebook.com/{META_API_VERSION}/{PIXEL_ID}/events'

def hash_data(data):
    """Função para hashear os dados do usuário com SHA-256 (requisito do Meta)."""
    if data is None or data == "":
        return None
    # Meta requer normalização: minúsculas e sem espaços extras
    return hashlib.sha256(str(data).lower().strip().encode('utf-8')).hexdigest()

def normalize_city(city):
    """Normaliza o nome da cidade."""
    if not city:
        return None
    return str(city).lower().strip().replace(" ", "")

def normalize_state(state):
    """Normaliza o estado."""
    if not state:
        return None
    return str(state).lower().strip()

def normalize_country(country):
    """Normaliza o código do país."""
    if not country:
        return None
    if len(str(country)) > 2:
        return 'br' 
    return str(country).lower().strip()

def send_event_to_meta(event_name, user_data, custom_data=None, event_source_url=None, action_source='website', event_id=None, event_time=None):
    """Monta e envia um evento para a API de Conversões do Meta (CAPI)."""
    if not ACCESS_TOKEN or not PIXEL_ID:
        logger.error("Credenciais do Meta não configuradas.")
        return None
    
    try:
        if not event_id:
            event_id = str(uuid.uuid4())
        
        if event_time is None:
            event_time = int(time.time())
        else:
            try:
                # Goxgain envia em milissegundos, converter para segundos
                if float(event_time) > 1e10: 
                    event_time = int(float(event_time) / 1000)
                else:
                    event_time = int(float(event_time))
            except:
                event_time = int(time.time())

        event_payload = {
            'event_name': event_name,
            'event_time': event_time,
            'event_id': event_id,
            'event_source_url': event_source_url,
            'action_source': action_source,
            'user_data': user_data,
        }

        if custom_data:
            event_payload['custom_data'] = custom_data

        final_payload = {
            'data': [event_payload],
            'access_token': ACCESS_TOKEN
        }
        
        logger.info(f"Enviando evento '{event_name}' para Meta CAPI (Goxgain)")
        logger.debug(f"Payload completo: {json.dumps(final_payload, indent=2)}")
        
        response = requests.post(META_API_URL, json=final_payload)
        
        logger.info(f"Resposta Meta CAPI ({event_name}): {response.status_code} - {response.text}")
        
        return response
        
    except Exception as e:
        logger.error(f"Erro ao enviar evento '{event_name}': {str(e)}")
        return None

def extract_client_ip(payload, request_obj):
    """Extrai IP do payload da Goxgain ou request."""
    # Goxgain manda 'ip' na raiz
    ip = payload.get('ip')
    
    if not ip:
        if request_obj.headers.getlist("X-Forwarded-For"):
            ip = request_obj.headers.getlist("X-Forwarded-For")[0]
        else:
            ip = request_obj.remote_addr
            
    return ip

def prepare_user_data(payload, request_obj):
    """Prepara user_data baseado na estrutura da Goxgain."""
    user_data = {}
    
    # Goxgain tem objeto 'user' aninhado
    user_info = payload.get('user', {})
    
    # 1. Email (em)
    email = user_info.get('email')
    if email:
        user_data['em'] = hash_data(email)
        
    # 2. Phone (ph)
    phone = user_info.get('phone')
    if phone:
        clean_phone = ''.join(filter(str.isdigit, str(phone)))
        user_data['ph'] = hash_data(clean_phone)

    # 3. Nomes (fn, ln)
    first_name = user_info.get('first_name')
    # Goxgain não manda explicitamente sobrenome separado nos exemplos, mas pode vir.
    # Se 'username' for composto, poderiamos tentar separar, mas melhor confiar no first_name se houver.
    # O exemplo user tem "username", "first_name".
    if first_name:
        user_data['fn'] = hash_data(first_name)
    
    # Tentativa de sobrenome se não tiver campo explícito:
    # Se o username parecer um nome completo, poderíamos tentar split, mas é arriscado (nickname).
    
    # 4. External ID
    user_id = user_info.get('id')
    if user_id:
        user_data['external_id'] = hash_data(user_id)
      
    # 5. Client IP Address (não hash)
    ip_address = extract_client_ip(payload, request_obj)
    if ip_address:
        user_data['client_ip_address'] = ip_address
        
    # 6. User Agent (não hash)
    # Goxgain não manda user agent no JSON, tentar do header
    ua = request_obj.headers.get('User-Agent')
    if ua:
        user_data['client_user_agent'] = ua
        
    # 7. fbc / fbp (Cookies)
    # Goxgain não documentou fbc/fbp no payload, mas vamos tentar pegar de headers/cookies do request webhook?
    # Webhooks são server-to-server, então não trazem cookies do usuário.
    # Se Goxgain não repassar, não temos como pegar.
    # O payload não listou fbc/fbp.
    
    return user_data

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def goxgain_webhook():
    """Endpoint para webhooks da Goxgain."""
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "No JSON payload received"}), 400
            
        logger.info(f"Goxgain Webhook recebido: {payload}")
        
        event_type = payload.get('event')
        if not event_type:
            return jsonify({"error": "Missing 'event' field"}), 400

        user_data = prepare_user_data(payload, request)
        
        # Como Goxgain não envia a URL da página no payload, podemos deixar vazio ou por a home
        event_source_url = "https://goxgain.com" # Placeholder ou deixar None
        
        event_time = payload.get('time') # Milissegundos
        event_id_unique = str(uuid.uuid4())

        # --- EVENTOS GOXGAIN ---
        
        # 1. Cadastro: "register" -> CompleteRegistration + ViewContent
        if event_type == 'register':
            custom_data = {
                'currency': 'BRL',
                'value': 0.0,
                'content_name': 'Registration'
            }
            # Evento Principal: CompleteRegistration
            send_event_to_meta('CompleteRegistration', user_data, custom_data, event_source_url, event_id=event_id_unique, event_time=event_time)
            
            # Evento Secundário (Qualidade): ViewContent
            vc_data = {
                'currency': 'BRL',
                'value': 0.0,
                'content_name': 'Registration Success Page'
            }
            send_event_to_meta('ViewContent', user_data, vc_data, event_source_url, event_id=str(uuid.uuid4()), event_time=event_time)
            
        # 2. Login: "login" -> Lead + ViewContent
        elif event_type == 'login':
            custom_data = {
                'currency': 'BRL',
                'value': 0.0,
                'content_name': 'Login'
            }
            # Evento Principal: Lead
            send_event_to_meta('Lead', user_data, custom_data, event_source_url, event_id=event_id_unique, event_time=event_time)

            # Evento Secundário (Qualidade): ViewContent
            vc_data = {
                'currency': 'BRL',
                'value': 0.0,
                'content_name': 'Login Page View'
            }
            send_event_to_meta('ViewContent', user_data, vc_data, event_source_url, event_id=str(uuid.uuid4()), event_time=event_time)
            
        # 3. Criação de Depósito: "deposit_created" -> InitiateCheckout E AddToCart
        elif event_type == 'deposit_created':
            amount = payload.get('amount', 0)
            currency = payload.get('currency', 'BRL')
            internal_id = payload.get('internal_id')
            
            # --- AddToCart ---
            atc_data = {
                'currency': currency,
                'value': float(amount),
                'content_ids': [str(internal_id)],
                'content_type': 'product',
                'content_name': 'Deposit Created'
            }
            send_event_to_meta('AddToCart', user_data, atc_data, event_source_url, event_id=str(uuid.uuid4()), event_time=event_time)
            
            # --- InitiateCheckout ---
            ic_data = {
                'currency': currency,
                'value': float(amount),
                'content_ids': [str(internal_id)],
                'content_type': 'product',
                'num_items': 1
            }
            send_event_to_meta('InitiateCheckout', user_data, ic_data, event_source_url, event_id=event_id_unique, event_time=event_time)
            
        # 4. Depósito Pago: "deposit_paid" -> Purchase
        elif event_type == 'deposit_paid':
            amount = payload.get('amount', 0)
            currency = payload.get('currency', 'BRL')
            internal_id = payload.get('internal_id')
            
            purchase_data = {
                'currency': currency,
                'value': float(amount),
                'content_ids': [str(internal_id)],
                'content_type': 'product',
                'content_name': 'Deposit Paid'
            }
            
            send_event_to_meta('Purchase', user_data, purchase_data, event_source_url, event_id=event_id_unique, event_time=event_time)
            
        else:
            logger.info(f"Evento {event_type} não mapeado.")
            return jsonify({"status": "ignored", "message": f"Event {event_type} not mapped"}), 200

        return jsonify({"status": "success", "message": "Event processed"}), 200

    except Exception as e:
        logger.error(f"Erro no webhook: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "Goxgain Meta CAPI"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
