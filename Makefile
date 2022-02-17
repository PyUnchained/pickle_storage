test:
	export PICKLE_STORAGE_SETTINGS=pickle_storage.tests.settings && \
	coverage run --source=. --omit="*/tests/*" -m pytest ./pickle_storage/tests && \
	rm -rf ./test_data_dir && coverage html