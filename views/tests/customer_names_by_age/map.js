function(doc) {
  if (doc.type === 'customer') {
    emit(doc.age, doc.name);
  }
}
