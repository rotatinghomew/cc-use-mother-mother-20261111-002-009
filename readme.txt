测试：python3 -m unittest discover -s tests/federated_trials -p 'test_*.py'
构建：python3 -m compileall -q trial_site_federator
验收：python3 -m trial_site_federator.console.federate fixtures/demo.json --db /tmp/trial_site_federator-acceptance.sqlite3
