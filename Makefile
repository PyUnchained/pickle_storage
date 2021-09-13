test:
	export PICKLE_STORAGE_SETTINGS=pickle_storage.config.test_settings && \
	coverage run --source=. --omit="*/tests/*" -m pytest tests   && rm -rf ./test_data_dir \
	&& coverage html