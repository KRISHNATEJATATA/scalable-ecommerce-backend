"""
RFC 9457 Problem Details builder.

Constructs standardized error responses following the API contract's flat
Problem Details shape:

    {"type", "status", "title", "detail", "trace_id", "details"}
"""
