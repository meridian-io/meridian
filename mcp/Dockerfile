FROM gcr.io/distroless/static:nonroot
COPY meridian /meridian
ENTRYPOINT ["/meridian"]
CMD ["--transport", "sse", "--addr", ":8080"]
