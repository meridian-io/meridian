FROM gcr.io/distroless/static:nonroot
COPY meridian /meridian
ENTRYPOINT ["/meridian"]
CMD ["--transport", "http", "--addr", ":8080"]
