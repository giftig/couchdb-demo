function(doc) {
  if (doc.type === 'visit') {
    emit([doc.customer, doc.timestamp], {_id: doc.customer, doc.ip_address});
  }
}
