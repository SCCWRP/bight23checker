{
    console.log("Hello world")
    // This is for the select dropdowns that are not dependent on other select items
    Array.from(document.querySelectorAll('select.dynamic-select-item[data-index]')).forEach(async function(elem, i, arr) {
        
        let valuesField = elem.dataset.optionValuesField;
        let displayField = elem.dataset.optionDisplayField;
        let table = elem.dataset.optionTable;
        
        if (Number(elem.dataset.index) >= 1){
            elem.addEventListener('change', async function(e){
                
                nextElem = elem.parentElement.parentElement.querySelector(`select.dynamic-select-item[data-index="${Number(elem.dataset.index) + 1}"]`)

                if (nextElem) {
                    let previousElements = arr.filter(d => (d.dataset.index < nextElem.dataset.index) && (d.dataset.index >= 1) )
                    .map(d => {
                        return new Object({
                            "value": d.value, // the value of the select item
                            "fieldname" : d.dataset.optionValuesField // the name attribute of the select item
                        })
                    })
                    .map(d => {
                        return `${d.fieldname}=${d.value}`
                    })
                    .join('&')
                    
                    console.log("previousElements")
                    console.log(previousElements)
                    
                    let resp = await fetch(`
                        /${script_root}/login_values?valuefield=${nextElem.dataset.optionValuesField}&displayfield=${nextElem.dataset.optionDisplayField}&table=${nextElem.dataset.optionTable}&${previousElements}
                    `);
                    
                    let data = await resp.json()

                    nextElem.innerHTML = `<option value="none" selected disabled hidden></option>`;
                    data.data.forEach(d => {
                        nextElem.innerHTML += `
                            <option value="${d.actualvalue}">${d.displayvalue}</option>
                        `
                    })
                }
            })

            // after adding the event listener, we shouldnt populate the select items of the select items that depend on previous ones
            if (Number(elem.dataset.index) > 1){
                return;
            }

        }

        let resp = await fetch(`
            /${script_root}/login_values?valuefield=${valuesField}&displayfield=${displayField}&table=${table}
        `);

        let data = await resp.json();
        elem.innerHTML = `<option value="none" selected disabled hidden></option>`;
        data.data.forEach(d => {
            elem.innerHTML += `
                <option value="${d.actualvalue}">${d.displayvalue}</option>
            `
        })
    })
}