from hvac.api.auth_methods import Kubernetes, Userpass
from playwright.sync_api import sync_playwright, BrowserContext
import hvac
import hvac.exceptions
import os
import sentry_sdk

sentry_sdk.init()

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0'
client = None

def vault_login():
    global client
    vault_url = os.environ['VAULT_ENDPOINT']
    if 'KUBERNETES_SERVICE_HOST' in os.environ:
        client = hvac.Client(url=vault_url)
        f = open('/var/run/secrets/kubernetes.io/serviceaccount/token')

        jwt = f.read()

        Kubernetes(client.adapter).login(
            role=os.environ['VAULT_ROLE'],
            jwt=jwt
        )
    else:
        raise Exception("Not yet implemented")

def vault_get_credentials(name: str) -> dict[str, str]:
    if client == None:
        vault_login()
    return client.secrets.kv.read_secret(path=name, mount_point='data')['data']['data']

def save_cookies(context: BrowserContext, path: str):
    # Get the current storage state (including cookies).
    context = context.storage_state()
    context['cookies'].sort(key=lambda x: f"{x['domain']}:{x['name']}")
    for origin in context['origins']:
        origin['localStorage'].sort(key=lambda x: x['name'])
    client.secrets.kv.v2.create_or_update_secret(path=path, mount_point='data', secret=context)

def load_enriched_context(browser, path: str) -> BrowserContext:
    try:
        secret = client.secrets.kv.v2.read_secret(path=path, mount_point='data')['data']['data']
        return browser.new_context(storage_state = secret, user_agent = USER_AGENT)
    except hvac.exceptions.InvalidPath:
        return browser.new_context(user_agent = USER_AGENT)

class SecureBrowser(object):
    def __init__(self, cookie_store_name: str, trace_name: str=None):
        self.cookie_name = cookie_store_name
        self.trace_name = trace_name

    def __enter__(self) -> BrowserContext:
        vault_login()
        self.p = sync_playwright().start()
        self.is_prod = 'KUBERNETES_SERVICE_HOST' in os.environ

        self.browser = self.p.firefox.launch(headless=self.is_prod) # TODO: TOGGLE this
        print("browser opened")
        self.context = load_enriched_context(self.browser, self.cookie_name)
        # Bot detection evasion - Hide this easy to detect property
        # https://www.zenrows.com/blog/avoid-playwright-bot-detection
        self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.context.tracing.start(screenshots=True, snapshots=True, sources=True)
        return self.context

    def __exit__(self, *args):
        if self.trace_name:
            self.context.tracing.stop(path = f"out/{self.trace_name}.zip")
        self.browser.close()
        if self.is_prod:
            client.logout(revoke_token=True)
