from argparse import ArgumentError
import ssl
from django.db.models import Avg
from datetime import timedelta, datetime
from receiver.models import Data, Measurement
import paho.mqtt.client as mqtt
import schedule
import time
from django.conf import settings
from django.utils import timezone
import time as pytime

client = mqtt.Client(settings.MQTT_USER_PUB)

# Guarda el último estado enviado por estación (para no publicar repetido)
LAST_LED_STATE = {}  # key: (country, state, city, user) -> "LED_ON"/"LED_OFF"

def analyze_data():
    # Consulta todos los datos de la última hora, los agrupa por estación y variable
    # Compara el promedio con los valores límite que están en la base de datos para esa variable.
    # Si el promedio se excede de los límites, se envia un mensaje de alerta.

    print("Calculando alertas...")

    data = Data.objects.filter(
        base_time__gte=datetime.now() - timedelta(hours=1))
    aggregation = data.annotate(check_value=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state',
                        'station__location__country') \
        .values('check_value', 'station__user__username',
                'measurement__name',
                'measurement__max_value',
                'measurement__min_value',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name')
    alerts = 0
    for item in aggregation:
        alert = False

        variable = item["measurement__name"]
        max_value = item["measurement__max_value"] or 0
        min_value = item["measurement__min_value"] or 0

        country = item['station__location__country__name']
        state = item['station__location__state__name']
        city = item['station__location__city__name']
        user = item['station__user__username']

        if item["check_value"] > max_value or item["check_value"] < min_value:
            alert = True

        if alert:
            message = "ALERT {} {} {}".format(variable, min_value, max_value)
            topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
            print(datetime.now(), "Sending alert to {} {}".format(topic, variable))
            client.publish(topic, message)
            alerts += 1

    print(len(aggregation), "dispositivos revisados")
    print(alerts, "alertas enviadas")
    analyze_led_from_db()
    
def analyze_led_temp_5min():
    """
    Evento: promedio de temperatura últimos 5 minutos > max_value (en Measurement)
    Acción: publicar LED_ON / LED_OFF al tópico .../<user>/in
    """
    print("Calculando evento LED por temperatura promedio (5 min)...")
    try:
        latest = Data.objects.order_by('-id').values_list('time', flat=True).first()
        digits = len(str(latest)) if latest else 13  # fallback

        now = pytime.time()

        if digits <= 10:
            # segundos
            cutoff_epoch = int(now) - 5*60
        elif digits <= 13:
            # milisegundos
            cutoff_epoch = int(now * 1_000) - 5*60*1_000
        elif digits <= 16:
            # microsegundos
            cutoff_epoch = int(now * 1_000_000) - 5*60*1_000_000
        else:
            # nanosegundos (o similar)
            cutoff_epoch = int(now * 1_000_000_000) - 5*60*1_000_000_000

        print("DEBUG time digits =", digits, "latest =", latest, "cutoff =", cutoff_epoch)

        # Trae datos de los últimos 5 minutos solo para temperatura
        data_qs = Data.objects.filter(
            time__gte=cutoff_epoch,
            measurement__name__iexact="temperatura"
        )

        # Agrupa por estación/usuario y ubicación, calcula promedio
        aggregation = (
            data_qs
            .select_related('station', 'measurement')
            .select_related('station__user', 'station__location')
            .select_related('station__location__city', 'station__location__state', 'station__location__country')
            .values(
                'station__user__username',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name',
                'measurement__max_value',
            )
            .annotate(avg_temp=Avg('avg_value'))
        )

        processed = 0
        changes = 0

        for item in aggregation:
            processed += 1

            user = item['station__user__username']
            city = item['station__location__city__name']
            state = item['station__location__state__name']
            country = item['station__location__country__name']

            avg_temp = item['avg_temp'] or 0.0
            threshold = item['measurement__max_value'] or 0.0

            # Si no hay umbral configurado, por seguridad no prendemos el LED
            if threshold <= 0:
                desired = "LED_OFF"
            else:
                desired = "LED_ON" if avg_temp > threshold else "LED_OFF"

            topic = f"{country}/{state}/{city}/{user}/in"

            key = (country, state, city, user)
            last = LAST_LED_STATE.get(key)

            # Solo publica si cambió el estado
            if last != desired:
                print(datetime.now(), f"avg_temp={avg_temp:.2f} threshold={threshold:.2f} -> {desired} to {topic}")
                client.publish(topic, desired)
                LAST_LED_STATE[key] = desired
                changes += 1

        print(f"{processed} estaciones revisadas. {changes} cambios LED publicados.")
        
    except Exception as e:
        print("Error en analyze_led_temp_5min:", e)
        
        
def analyze_led_from_db():
    """
    Condición (con consulta a BD):
      - promedio de temperatura (última hora) por estación > measurement.max_value
    Acción:
      - publicar LED_ON / LED_OFF a .../<user>/in
    """
    print("Calculando evento LED (DB) por promedio de temperatura...")

    cutoff = timezone.now() - timedelta(hours=1)

    qs = (
        Data.objects.filter(
            base_time__gte=cutoff,
            measurement__name__iexact="temperatura"
        )
        .select_related('station__user', 'station__location__city',
                        'station__location__state', 'station__location__country',
                        'measurement')
        .values(
            'station__user__username',
            'station__location__city__name',
            'station__location__state__name',
            'station__location__country__name',
            'measurement__max_value',
        )
        .annotate(avg_temp=Avg('avg_value'))
    )

    processed = 0
    changes = 0

    for item in qs:
        processed += 1

        user = item['station__user__username']
        city = item['station__location__city__name']
        state = item['station__location__state__name']
        country = item['station__location__country__name']

        avg_temp = float(item['avg_temp'] or 0.0)
        threshold = float(item['measurement__max_value'] or 0.0)

        # si no hay threshold, mantenemos OFF
        desired = "LED_ON" if (threshold > 0 and avg_temp > threshold) else "LED_OFF"

        topic = f"{country}/{state}/{city}/{user}/in"

        key = (country, state, city, user)
        last = LAST_LED_STATE.get(key)

        if last != desired:
            print(datetime.now(), f"LED(DB): avg={avg_temp:.2f} thr={threshold:.2f} -> {desired} to {topic}")
            client.publish(topic, desired)
            LAST_LED_STATE[key] = desired
            changes += 1

    print(f"{processed} estaciones revisadas. {changes} cambios LED publicados.")


def on_connect(client, userdata, flags, rc):
    '''
    Función que se ejecuta cuando se conecta al bróker.
    '''
    print("Conectando al broker MQTT...", mqtt.connack_string(rc))


def on_disconnect(client: mqtt.Client, userdata, rc):
    '''
    Función que se ejecuta cuando se desconecta del broker.
    Intenta reconectar al bróker.
    '''
    print("Desconectado con mensaje:" + str(mqtt.connack_string(rc)))
    print("Reconectando...")
    client.reconnect()


def setup_mqtt():
    '''
    Configura el cliente MQTT para conectarse al broker.
    '''

    print("Iniciando cliente MQTT...", settings.MQTT_HOST, settings.MQTT_PORT)
    global client
    try:
        client = mqtt.Client(settings.MQTT_USER_PUB)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        if settings.MQTT_USE_TLS:
            client.tls_set(ca_certs=settings.CA_CRT_PATH,
                           tls_version=ssl.PROTOCOL_TLSv1_2, cert_reqs=ssl.CERT_NONE)

        client.username_pw_set(settings.MQTT_USER_PUB,
                               settings.MQTT_PASSWORD_PUB)
        client.connect(settings.MQTT_HOST, settings.MQTT_PORT)
        client.loop_start()

    except Exception as e:
        print('Ocurrió un error al conectar con el bróker MQTT:', e)


def start_cron():
    '''
    Inicia el cron que se encarga de ejecutar la función analyze_data cada 5 minutos.
    '''
    print("Iniciando cron...")
    schedule.every(1).minutes.do(analyze_data)
    #schedule.every(1).minutes.do(analyze_led_temp_5min)
    print("Servicio de control iniciado")
    while 1:
        schedule.run_pending()
        time.sleep(1)
