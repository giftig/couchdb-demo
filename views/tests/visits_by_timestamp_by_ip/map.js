function(doc) {
  if (doc.type === 'visit') {
    emit([doc.timestamp, doc.ip_address], {_id: doc.customer});
  }
}
