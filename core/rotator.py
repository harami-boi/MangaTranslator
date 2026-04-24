import threading
import copy
from pathlib import Path
from utils.logging import log_message

class RotatorSession:
    def __init__(self, initial_config):
        self.lock = threading.Lock()
        self.api_keys = []
        
        # Try to load from api_keys.txt
        keys_path = Path('api_keys.txt')
        if keys_path.exists():
            try:
                with open(keys_path, 'r', encoding='utf-8') as f:
                    self.api_keys = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            except Exception as e:
                log_message(f"Failed to read api_keys.txt: {e}")
        
        # Fallback to config key
        if not self.api_keys and initial_config.translation.google_api_key:
            self.api_keys = [initial_config.translation.google_api_key]

        # Model Fallback Chain
        self.model_chain = [
            "gemini-2.5-flash-lite",
            "gemini-2.0-flash-lite",
            "gemini-2.5-flash",
            "gemini-2.0-flash"
        ]
        
        self.key_idx = 0
        self.model_idx = 0
        self.exhausted_keys = set()
        self.dead_models = set()
        self.max_retries = 3

    def get_config(self):
        with self.lock:
            # Find available model
            model = None
            for i in range(self.model_idx, len(self.model_chain)):
                if self.model_chain[i] not in self.dead_models:
                    model = self.model_chain[i]
                    break
            
            # Find available key
            key = None
            if self.api_keys:
                for _ in range(len(self.api_keys)):
                    if (self.key_idx % len(self.api_keys)) not in self.exhausted_keys:
                        key = self.api_keys[self.key_idx % len(self.api_keys)]
                        break
                    self.key_idx += 1
            
            return model, key

    def advance_key(self):
        with self.lock:
            if self.api_keys:
                self.exhausted_keys.add(self.key_idx % len(self.api_keys))
                self.key_idx += 1

    def advance_model(self):
        with self.lock:
            model = None
            for i in range(self.model_idx, len(self.model_chain)):
                if self.model_chain[i] not in self.dead_models:
                    model = self.model_chain[i]
                    break
            if model:
                self.dead_models.add(model)
            self.model_idx += 1
            self.exhausted_keys.clear()
            self.key_idx = 0

    def reset_exhaustion(self):
        with self.lock:
            self.exhausted_keys.clear()
            self.dead_models.clear()
            self.model_idx = 0
            self.key_idx = 0

def translate_with_retry(img_path, config, output_path, rotator, cancellation_manager=None, display_path=""):
    RATE_LIMIT_KW = ['429', 'Rate limit', 'RESOURCE_EXHAUSTED', 'Resource Exhausted', 'quota']
    MODEL_ERR_KW = ['not found', 'does not exist', 'is not available', 'unsupported model', 'invalid model']
    
    last_err = None
    for attempt in range(rotator.max_retries * len(rotator.model_chain)):
        model, key = rotator.get_config()
        if not model:
            raise Exception("All models exhausted in fallback chain")
        if not key:
            rotator.advance_model()
            model, key = rotator.get_config()
            if not model:
                raise Exception("All keys and models exhausted")
            if not key:
                raise Exception("No available API keys")

        local_config = copy.deepcopy(config)
        # Only override if it's a Google provider, otherwise leave as is
        if local_config.translation.provider == "Google":
            local_config.translation.model_name = model
            local_config.translation.google_api_key = key

        try:
            from core.pipeline import translate_and_render
            translate_and_render(img_path, local_config, output_path, cancellation_manager=cancellation_manager)
            return True
        except Exception as e:
            err = str(e)
            last_err = err
            
            if local_config.translation.provider != "Google":
                # Don't rotate for non-Google providers, just fail
                raise
                
            if any(kw in err for kw in RATE_LIMIT_KW):
                log_message(f"[{display_path}] Key rate limited, rotating API key...", always_print=True)
                rotator.advance_key()
            elif any(kw.lower() in err.lower() for kw in MODEL_ERR_KW):
                log_message(f"[{display_path}] Model {model} unavailable, falling back...", always_print=True)
                rotator.advance_model()
            else:
                raise  # Other error, bubble up

    raise Exception(f"Max retries exceeded. Last error: {last_err}")
