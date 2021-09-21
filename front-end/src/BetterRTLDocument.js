import React from 'react';
import Markup from 'interweave';

function BetterRTLDocument(props) {
  const display_doc = (content_string) => {
    let obj = null;
    try {
      obj = JSON.parse(content_string);
    } catch (error) {
      console.error(error);
      return '';
    }

    let meta = obj['derived-metadata'];
    meta.text =  meta.text.replace( /<body.*?>/,'<div>').replace(/<\/body.*?>/,'</div>')
    let doctext = (<Markup tagName="div"
    className="text-wrap article-text"
    content={meta.text}/>)

    return (
      <div>
        <h1 dir="rtl" className="text-right"> {props.title}... </h1>
        <p> (best guess on publication date is '{props.date}')</p>
        <p><strong> {obj['WARC-Target-URI']} </strong></p>
        <p dir="rtl" className="text-right article-text"> {doctext} </p>
        <p><strong> ({meta['warc-file']})</strong></p>
      </div>
    );
  };

  if (props.content) {	
	return (
	  <>{display_doc(props.content)}</>
	);
  } else {
	return (<p>waiting for document...</p>);
  }
}

export { BetterRTLDocument as default };
