function(doc) {
  if (doc.type === 'customer') {
    emit(doc.weight, doc.height);
  }
}
