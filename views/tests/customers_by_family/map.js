function(doc) {
  if (doc.type === 'customer') {
    emit(doc.family, null);
  }
}
