# tap-chatgpt

A read-only Singer tap for the [OpenAI Advertiser API](https://developers.openai.com/ads/api-overview). It follows every page of campaigns, ad groups, and ads returned for one Ads Manager account. Each sync is a full refresh. The ads stream contains the original ad and creative metadata, including any returned image URLs, in `payload`. The tap emits JSON records and leaves image files at their source URLs.

## Meltano setup

An account-scoped Advertiser API key is available in Ads Manager settings. Set `OPENAI_ADS_API_KEY` in the environment that runs Meltano. Configure one tap instance per account if you need to sync multiple accounts.

Add this entry under `plugins.extractors` in your Meltano project:

```yaml
- name: tap-chatgpt
  namespace: tap_chatgpt
  pip_url: git+https://github.com/siilats/tap-chatgpt.git
  executable: tap-chatgpt
  capabilities:
  - discover
  - catalog
```

Run these commands from the Meltano project directory:

```sh
meltano install extractor tap-chatgpt
meltano invoke tap-chatgpt --discover
meltano run tap-chatgpt YOUR_EXISTING_LOADER
```

For a local checkout, set `pip_url` to the checkout's absolute path. You can also run `python -m tap_chatgpt --discover` directly from the checkout. Running without `--discover` streams Singer messages to stdout. Pass `--config` with an `api_key` field if your runner supplies credentials through a private Singer config file.
