function(doc) {
  if (doc.type === 'visit') {
    emit(doc.timestamp, {_id: doc.customer});
  }
}
