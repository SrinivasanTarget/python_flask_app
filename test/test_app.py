def test_users(app, client):
    response = client.get('/users')
    assert response.status_code == 200