# Troubleshooting

## Connection Issues

### Cannot connect to broker
- Verify the broker address and port (default: 9092)
- Check firewall rules allow outbound TCP on port 9092
- If using TLS, ensure certificates are valid and not expired

### Authentication failures
- Verify SASL credentials are correct
- Check the SASL mechanism matches the broker configuration
- Ensure the user has appropriate ACL permissions

## Producer Issues

### Messages not being delivered
- Check that the topic exists (auto-create may be disabled)
- Verify the producer is calling `flush()` before shutdown
- Monitor for `ProducerError` exceptions in logs

## Consumer Issues

### Consumer not receiving messages
- Verify the consumer group ID is correct
- Check that the topic has messages (use admin client to inspect)
- Ensure the consumer offset is set correctly (earliest vs latest)
