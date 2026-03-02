FROM gcr.io/distroless/static:nonroot
COPY meridian-mcp /meridian-mcp
ENTRYPOINT ["/meridian-mcp"]
CMD ["--transport", "sse", "--addr", ":8080"]
